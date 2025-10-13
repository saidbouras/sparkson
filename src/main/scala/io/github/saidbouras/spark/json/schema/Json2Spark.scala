package io.github.saidbouras.spark.json.schema

import io.circe._
import io.circe.parser._
import io.github.saidbouras.spark.json.schema.Json2Spark.{extractType, hasCycle, jsonContainsCycle}
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe._

object Json2Spark {

  /*
   * Given a file path, convert the file resource to a string
   */
  def readFile(fqPath: String): String = {
    val file = scala.io.Source.fromFile(fqPath)
    try file.mkString
    finally file.close()
  }

  /*
   * Given a cursor location, return list of required fields,
   *   The "required" key is located at each level of a json object. If missing or empty None is returned
   */
  def requiredFields(c: ACursor): Option[Seq[String]] = {
    c.downField("required").as[Seq[String]] match {
      case Left(e) => None
      case Right(v) => Some(v)
    }
  }

  /**
   * Returns current path of cursor in a Json schema
   *
   * @param c
   * @return
   */
  def cursorPath(c: ACursor): String = {
    c.key match {
      case None => "#" //base case
      case Some(entry) => cursorPath(c.up) + "/" + entry
    }
  }

  /**
   * Metadata is the full path of the cursor(lineage).
   * In Spark's ArrayType, this cannot be populated and therefore must be maintained from the parent
   * e.g. why we need to pass in additional param to maintain lineage inside a json resource
   *
   * @param path
   * @param description
   * @return
   */
  def metadata(path: String, description: String = ""): Metadata = {
    Metadata.fromJson(
      """
       {
         "path": """" + path +
        """",
         "description": """ + Literal(Constant(description)).toString.replace("\\'", "") +
        """
       }
      """)
  }

  /*
   * Mapping json simple datatypes to spark datatypes
   */
  val TypeMapping: Map[String, _ >: DataType] = Map(
    "string" -> StringType,
    "decimal" -> DecimalType,
    "number" -> DoubleType,
    "float" -> FloatType,
    "integer" -> LongType,
    "boolean" -> BooleanType,
    "timestamp" -> TimestampType,
    "date" -> DateType
  )

  /**
   * CHANGE 1: Completely rewritten cycle detection algorithm
   * New approach: Track visited schema paths during traversal to detect cycles at any depth
   *
   * @param path File path to JSON schema
   * @return true if schema contains cycles, false otherwise
   */
  def jsonContainsCycle(path: String): Boolean = {
    parse(readFile(path)) match {
      case Left(_) => throw new Exception("JSON schema invalid")
      case Right(json) => hasCycle(json)
    }
  }

  /**
   * CHANGE 2: New helper method - checks if JSON schema has cycles
   * Uses set-based tracking to detect when we revisit a schema path
   *
   * @param json The parsed JSON schema
   * @return true if cycles exist
   */
  def hasCycle(json: Json): Boolean = {
    val rootCursor = json.hcursor

    // Main traversal function with visited path tracking
    def traverse(cursor: ACursor, visited: Set[String], currentPath: String): Boolean = {
      // Check for $ref first
      cursor.downField("$ref").as[String] match {
        case Right(ref) =>
          // Resolve the reference to an absolute path
          val resolvedPath = resolveRef(ref)

          // Cycle detected if we've already visited this reference
          if (visited.contains(resolvedPath)) {
            true
          } else {
            // Navigate to the referenced location and continue traversal
            navigateToRef(rootCursor, ref) match {
              case Some(refCursor) =>
                traverse(refCursor, visited + resolvedPath, resolvedPath)
              case None => false // Reference points nowhere, no cycle
            }
          }

        case Left(_) =>
          // No $ref, check nested structures
          val propsHaveCycle = cursor.downField("properties").keys match {
            case Some(propNames) =>
              propNames.exists { propName =>
                val propCursor = cursor.downField("properties").downField(propName)
                val propPath = s"$currentPath/properties/$propName"
                traverse(propCursor, visited + currentPath, propPath)
              }
            case None => false
          }

          val itemsHaveCycle = cursor.downField("items").focus match {
            case Some(itemValue) if itemValue.isObject =>
              val itemsCursor = cursor.downField("items")
              val itemsPath = s"$currentPath/items"
              traverse(itemsCursor, visited + currentPath, itemsPath)
            case _ => false
          }

          val additionalPropsHaveCycle = cursor.downField("additionalProperties").focus match {
            case Some(apValue) if apValue.isObject =>
              val apCursor = cursor.downField("additionalProperties")
              val apPath = s"$currentPath/additionalProperties"
              traverse(apCursor, visited + currentPath, apPath)
            case _ => false
          }

          val allOfHaveCycle = cursor.downField("allOf").focus match {
            case Some(allOfValue) if allOfValue.isArray =>
              val size = allOfValue.asArray.getOrElse(Vector.empty).size
              (0 until size).exists { idx =>
                val allOfCursor = cursor.downField("allOf").downN(idx)
                val allOfPath = s"$currentPath/allOf/$idx"
                traverse(allOfCursor, visited + currentPath, allOfPath)
              }
            case _ => false
          }

          propsHaveCycle || itemsHaveCycle || additionalPropsHaveCycle || allOfHaveCycle
      }
    }

    // Helper: Resolve a $ref to an absolute path
    def resolveRef(ref: String): String = {
      if (ref.startsWith("#/")) {
        ref // Already absolute
      } else if (ref == "#") {
        "#" // Root reference
      } else {
        // External or relative reference
        ref
      }
    }

    // Helper: Navigate cursor to a $ref location
    def navigateToRef(cursor: HCursor, ref: String): Option[ACursor] = {
      if (ref == "#") {
        Some(cursor) // Reference to root
      } else if (ref.startsWith("#/")) {
        // Navigate through the path segments
        val parts = ref.stripPrefix("#/").split("/")
        parts.foldLeft(Option(cursor: ACursor)) { (cursorOpt, part) =>
          cursorOpt.flatMap { c =>
            val nextCursor = c.downField(part)
            if (nextCursor.succeeded) Some(nextCursor) else None
          }
        }
      } else {
        None // External reference - not handled here
      }
    }

    // Check root properties
    val rootPropsHaveCycle = rootCursor.downField("properties").keys match {
      case Some(propNames) =>
        propNames.exists { propName =>
          val propCursor = rootCursor.downField("properties").downField(propName)
          val propPath = s"#/properties/$propName"
          traverse(propCursor, Set.empty, propPath)
        }
      case None => false
    }

    // Check definitions
    val definitionsHaveCycle = rootCursor.downField("definitions").keys match {
      case Some(defNames) =>
        defNames.exists { defName =>
          val defCursor = rootCursor.downField("definitions").downField(defName)
          val defPath = s"#/definitions/$defName"
          traverse(defCursor, Set.empty, defPath)
        }
      case None => false
    }

    // Check $defs (newer JSON Schema versions)
    val defsHaveCycle = rootCursor.downField("$defs").keys match {
      case Some(defNames) =>
        defNames.exists { defName =>
          val defCursor = rootCursor.downField("$defs").downField(defName)
          val defPath = s"#/$$defs/$defName"
          traverse(defCursor, Set.empty, defPath)
        }
      case None => false
    }

    rootPropsHaveCycle || definitionsHaveCycle || defsHaveCycle
  }

  /**
   * CHANGE 3: Deprecated - kept for backward compatibility but marked as deprecated
   * Use hasCycle() instead
   */
  @deprecated("Use hasCycle() instead - this method has limited depth detection", "1.0")
  def isPathContainsCycle(path: String): Boolean = {
    val refs = path.split("\\$ref").map(_.trim).filter(_.nonEmpty)
    if (refs.length < 2) return false
    val refPart = refs(1)
    refPart == "#"
  }

  /**
   * CHANGE 4: Deprecated - kept for backward compatibility but marked as deprecated
   * Use hasCycle() instead for proper cycle detection at any depth
   */
  @deprecated("Use hasCycle() instead - this method only checks fixed depth", "1.0")
  def pathContainsCycle(path: String, maxDepth: Integer = 2): Boolean = {
    val arr = path.split("\\$ref")
    !arr
      .filter(x => x.split("/").length > 4)
      .map(x => x.split("/")(3) + x.split("/")(4))
      .groupBy(identity)
      .mapValues(_.length)
      .filter(x => x._2 >= maxDepth)
      .isEmpty
  }

  /**
   * Extract type from JSON Schema, handling both String and Array (union types)
   * Returns (actualType, isNullableFromUnionType)
   *
   * @param c
   * @param defaultType
   * @return
   */
  def extractType(c: ACursor, defaultType: String = "string"): (String, Boolean) = {
    c.downField("type").focus match {
      case Some(json) if json.isString =>
        // Simple type: "type": "string"
        (json.asString.getOrElse(defaultType), false)

      case Some(json) if json.isArray =>
        // Union type: "type": ["string", "null"]
        val types = json.asArray.getOrElse(Vector.empty)
          .flatMap(_.asString)
        val hasNull = types.contains("null")
        val actualType = types.filterNot(_ == "null").headOption.getOrElse(defaultType)
        (actualType, hasNull)

      case _ => (defaultType, false) // CHANGE 5: Return default instead of throwing exception
    }
  }
}

/**
 * Representing a parser as
 *
 * @param rawJson              , the json represented as a string to convert to a spark schema
 * @param enforceRequiredField , enforce all required fields from the json schema in the conversion
 * @param defaultType          , is a dataType cannot be matched or converted it will be created as this dataType
 * @param circularReferences   , if there are self referencing types, populate this field with their path to avoid further expansions (and out of memory errors)
 * @param externalRefBaseURI   use when external references are not in the current working directory
 *     - this setting prepends a base uri to the external resource referenced
 */
class Json2Spark(rawJson: String,
                 enforceRequiredField: Boolean = true,
                 defaultType: String = "string",
                 defsLocation: String = "$defs", // CHANGE 6: Changed default from "$def" to "$defs" (standard)
                 circularReferences: Option[Seq[String]] = None,
                 externalRefBaseURI: String = "") {

  /**
   * Schema as a json object
   */
  val json: Json = parse(rawJson) match {
    case Left(e) => throw new Exception("Invalid JSON string: " + e)
    case Right(v) => v
  }

  /**
   * Function that returns all keys at a given path
   *
   * @param resourcePath
   * @return
   */
  def keys(resourcePath: String): Seq[String] = {
    cursorAt(resourcePath).keys.getOrElse(Seq.empty).toSeq
  }

  /**
   * See if the specified field name is required
   *
   * @param fieldName
   * @param rf
   * @return
   */
  def nullable(fieldName: String, rf: Option[Seq[String]]): Boolean = {
    rf match {
      case Some(x) =>
        !x.contains(fieldName) || !enforceRequiredField // CHANGE 7: Fixed logic - nullable when NOT required
      case None => true
    }
  }

  /**
   * Find circular references of a given resource
   *
   * @param resourcePath
   * @return
   */
  def isSelfReference(resourcePath: String): Seq[String] = {
    val c = cursorAt(resourcePath)
    c.downField("properties").keys
      .getOrElse(Seq.empty)
      .map(fieldName => c.downField("properties").downField(fieldName).downField("items").downField("$ref").as[String].getOrElse(""))
      .filter(path => path == resourcePath)
      .toSeq
  }

  /**
   * Return relevant struct for object referenced
   *
   * @return
   */
  def convert2Spark: StructType = {
    json.hcursor.downField("properties").keys match {
      case Some(x) =>
        StructType(
          x.map(fieldName =>
              property2Struct(
                json.hcursor.downField("properties").downField(fieldName),
                fieldName,
                Json2Spark.cursorPath(json.hcursor.downField("properties").downField(fieldName)),
                Json2Spark.requiredFields(json.hcursor))
            )
            .reduce((a, b) => a ++ b)
        )
      case None => throw new Exception("No properties found in json schema")
    }
  }

  def property2Struct(c: ACursor, fieldName: String, path: String, requiredFields: Option[Seq[String]] = None): Seq[StructField] = {
    c.keys match {
      case Some(x) if x.toSeq.contains("const") => Nil //const not supported in spark schema
      case Some(x) if x.toSeq.contains("$ref") => fieldName match {
        case "" => refs(c.downField("$ref").as[String].getOrElse(""), path, fieldName)
        case _ => refs(c.downField("$ref").as[String].getOrElse(""), path, fieldName) match {
          case x if x.size == 1 => x
          case x => Seq(StructField(fieldName, StructType(x)))
        }
      }
      // CHANGE 8: Simplified cycle check - removed pathContainsCycle, use isCircularReference only
      case Some(x) if (x.toSeq.contains("enum") || isCircularReference(c)) || hasCycle(c.focus.getOrElse(throw new Exception("JSON not valid")))=>
        Seq(StructField(fieldName,
          StringType,
          nullable(fieldName, requiredFields),
          Json2Spark.metadata(path, c.downField("description").as[String].getOrElse(""))))

      case Some(x) if x.toSeq.contains("type") =>
        // Extract type ONCE and use throughout
        val (fieldType, isNull) = Json2Spark.extractType(c, defaultType)

        fieldType match {
          case "string" | "number" | "float" | "integer" | "boolean" =>
            Seq(
              new StructField(
                fieldName,
                Json2Spark.TypeMapping.getOrElse(fieldType, StringType).asInstanceOf[DataType],
                isNull || nullable(fieldName, requiredFields),
                Json2Spark.metadata(path, c.downField("description").as[String].getOrElse(""))
              ))

          case "array" =>
            val itemsCursor = c.downField("items")

            property2Struct(itemsCursor, "", path + "/items", Json2Spark.requiredFields(itemsCursor)) match {
              case Nil =>
                Seq(
                  StructField(
                    fieldName,
                    ArrayType(Json2Spark.TypeMapping.getOrElse(defaultType, StringType).asInstanceOf[DataType]),
                    isNull || nullable(fieldName, requiredFields),
                    Json2Spark.metadata(path, c.downField("description").as[String].getOrElse("")))
                )
              case x if x.size == 1 =>
                Seq(
                  StructField(
                    fieldName,
                    ArrayType(x(0).dataType),
                    isNull || nullable(fieldName, requiredFields),
                    Json2Spark.metadata(path, c.downField("description").as[String].getOrElse("")))
                )
              case x if x.size > 1 =>
                Seq(
                  StructField(
                    fieldName,
                    ArrayType(new StructType(x.toArray)),
                    isNull || nullable(fieldName, requiredFields),
                    Json2Spark.metadata(path, c.downField("description").as[String].getOrElse("")))
                )
            }

          case "object" =>
            Seq(
              StructField(
                fieldName,
                new StructType({
                  c.downField("properties").keys match {
                    case Some(propKeys) =>
                      propKeys.map(fn =>
                          property2Struct(
                            c.downField("properties").downField(fn),
                            fn,
                            path + "/properties/" + fn,
                            Json2Spark.requiredFields(c)))
                        .reduce((a, b) => a ++ b).toArray
                    case None => throw new Exception("No properties found in json schema nested object path: " + path)
                  }
                }),
                isNull || nullable(fieldName, requiredFields),
                Json2Spark.metadata(path, c.downField("description").as[String].getOrElse(""))
              )
            )
        }
      case Some(x) if x.toSeq.contains("allOf") => //combine all references in a list as a single struct
        val size = c.downField("allOf").focus.flatMap(_.asArray).getOrElse(Vector.empty).size
        Seq(
          StructField(
            fieldName,
            StructType(
              (0 until size)
                .map(idx => {
                  property2Struct(c.downField("allOf").downN(idx), "", "/allOf")
                })
                .reduce((a, b) => a ++ b)),
            nullable(fieldName, requiredFields),
            Json2Spark.metadata(path, c.downField("description").as[String].getOrElse(""))
          )
        )
      case _ =>
        Seq(
          StructField(
            fieldName,
            Json2Spark.TypeMapping.getOrElse(defaultType, StringType).asInstanceOf[DataType],
            nullable(fieldName, requiredFields),
            Json2Spark.metadata(path, c.downField("description").as[String].getOrElse("")))
        )
    }
  }

  /**
   * Check if the cursor is cyclic
   *
   * @param c
   * @return
   */
  def isCircularReference(c: ACursor): Boolean = {
    circularReferences match {
      case Some(x) if x.contains(Json2Spark.cursorPath(c)) => true
      case _ => false
    }
  }

  /**
   * Place the cursor at a specific location. Assuming starts with "#"
   *
   * @param path
   * @return
   */
  def cursorAt(path: String): ACursor = {
    var c = json.hcursor.asInstanceOf[ACursor]
    for (y <- path.split('/').drop(1)) c = c.downField(y)
    c
  }

  /**
   * Returns a struct from a "$refs" mapping
   * (only supporting local refs now, e.g. begins with #
   *
   * @param resourcePath
   * @param basePath
   * @param fieldName
   * @return
   */
  def refs(resourcePath: String, basePath: String, fieldName: String): Seq[StructField] = {
    resourcePath.startsWith("#") match {
      case true => //This is a local resource in the same file
        val c = cursorAt(resourcePath)
        property2Struct(c, fieldName, basePath + "/" + "$ref//" + resourcePath, Json2Spark.requiredFields(c))
      case false => //This is an external resource e.g. file or https (not supporting https right now)
        resourcePath match {
          case x if x.contains("#") => //Ref is "file.json#/path/to/resource"
            val (fileName, location) = (x.split("#")(0), x.split("#")(1))
            val json = new Json2Spark(Json2Spark.readFile(externalRefBaseURI + "/" + fileName)
              , enforceRequiredField
              , defaultType
              , defsLocation
              , circularReferences
              , externalRefBaseURI)
            val c = json.cursorAt(location) // CHANGE 9: Use json instance's cursorAt
            json.property2Struct(c, fieldName, basePath + "/file:///" + location, Json2Spark.requiredFields(c))
          case x if x.contains("https") => ??? // Not implemented
          case x => //This is an entire separate json file
            new Json2Spark(Json2Spark.readFile(externalRefBaseURI + "/" + x)
              , enforceRequiredField
              , defaultType
              , defsLocation
              , circularReferences
              , externalRefBaseURI).convert2Spark
        }
    }
  }

  /**
   * Returns a struct from a "$defs" mapping
   *
   * @param resourceDefinition
   * @return
   */
  def defs(resourceDefinition: String): Seq[StructField] = {
    json.hcursor.downField(defsLocation).downField(resourceDefinition) match {
      case x if x.succeeded => property2Struct(x, resourceDefinition, "#/" + defsLocation + "/" + resourceDefinition, Json2Spark.requiredFields(x))
      case _ => Seq.empty // CHANGE 10: Return empty Seq instead of empty StructType
    }
  }
}