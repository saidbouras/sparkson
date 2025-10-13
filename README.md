# 🌟 Sparkson

![Build](https://github.com/saidbouras/sparkson/actions/workflows/build.yml/badge.svg)
![Release](https://github.com/saidbouras/sparkson/actions/workflows/release.yml/badge.svg)
![Maven Central](https://img.shields.io/maven-central/v/io.github.saidbouras/sparkson.svg)
![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)
![Scala](https://img.shields.io/badge/scala-2.12.19-lightgrey.svg)

> **Forked from** [Databricks Industry Solutions json2spark-schema](https://github.com/databricks-industry-solutions/json2spark-schema)  
> Maintained and extended by **Saïd Bouras**.

---

## 🚀 What is Sparkson?

**Sparkson** is a lightweight Scala library for Apache Spark that allows you to:

- Convert **JSON Schema** into **Spark StructType** easily 🛠️
- Perform **granular JSON validation** using UDFs ✅
- Extend with additional JSON utilities for Spark in the future 🌐

Think of it as a **bridge between JSON and Spark**: from schema inference to validation, all in a clean, reusable package.

---

## 📦 Features

- **JSON Schema → Spark StructType**: Automatically infer StructType from JSON Schema files.
- **JSON Validation (planned)**: Validate JSON records using Everit schema library via UDFs.
- **Maven Central Ready**: Classic JAR with proper dependencies, ready to be used as a library in your Spark projects.
- **Extensible**: Future modules for JSON utilities, validators, and more.

---

## 🧩 Example Usage

```scala
import io.github.saidbouras.spark.json.schema.JsonToStructType
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Sparkson Example")
  .getOrCreate()

// Load JSON Schema from file
val schema = JsonToStructType.fromFile("/path/to/schema.json")

// Apply to DataFrame
val df = spark.read.schema(schema).json("/path/to/json")
df.show()
```

## 🌱 Roadmap

- ✅ **v1.0**: JSON Schema → StructType (core functionality)
- ⚡  **v1.1**: JSON Validation UDFs (Everit)
- 🛠 **v1.2**: CLI tool for schema preview and quick validation
- 🔮 **Future**: Additional JSON utilities, modular Spark extensions

---

## 📌 Getting Started

### json2spark-schema

The first core functionality of Sparkson, initially forked from [Databricks Industry Solutions json2spark-schema](https://github.com/databricks-industry-solutions/json2spark-schema), allows you to **convert JSON Schema files into equivalent Spark StructType schemas**.

Key highlights:
- Supports **multiple JSON Schema specifications** in one project
- Handles  **nested and complex JSON structures**
- Generates **Spark-compatible StructType schemas** ready for DataFrame ingestion

#### Examples : 
```scala
import com.databricks.industry.solutions.json2spark._

val x = new Json2Spark(Json2Spark.file2String("src/test/resources/address.schema.json"))
x.convert2Spark
/*
org.apache.spark.sql.types.StructType = StructType
StructType(
  StructField(post-office-box,StringType,false), 
  StructField(extended-address,StringType,false), 
  StructField(street-address,StringType,false), 
  StructField(locality,StringType,true), 
  StructField(region,StringType,true), 
  StructField(postal-code,StringType,false), 
  StructField(country-name,StringType,true)
)
*/

```

#### Running with Spark
```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions.col

val spark = ( SparkSession.builder()
      .master("local[2]")
      .config("spark.driver.bindAddress","127.0.0.1") 
      .getOrCreate() )
      
val rdd = spark.sparkContext.parallelize(Seq( Seq("apple"), Seq("orange", "blueberry"), Seq("starfruit"), Seq("mango", "strawberry", "apple"))).map(row => Row(row))
val schema = new Json2Spark(Json2Spark.file2String("src/test/resources/veggies.json")).convert2Spark
val df = spark.createDataFrame(rdd, schema)

df.printSchema
/*
root
 |-- fruits: array (nullable = true)
 |    |-- element: string (containsNull = true)
 */

```

#### More Complex Representations 

Optionally allow flexibility in schema definition by not requiring fields to be populated

```scala
val x = new Json2Spark(Json2Spark.file2String("src/test/resources/address.schema.json"), enforceRequiredField=false)
x.convert2Spark
/*
org.apache.spark.sql.types.StructType = StructType
StructType(
  StructField(post-office-box,StringType,true), 
  StructField(extended-address,StringType,true), 
  StructField(street-address,StringType,true), 
  StructField(locality,StringType,true), 
  StructField(region,StringType,true), 
  StructField(postal-code,StringType,true), 
  StructField(country-name,StringType,true)
)
*/
```

#### Definition as a spark schema
Json schemas can represent objects in a definitions path. Using the ***defs*** method converts a json definition to a Spark Schema

```scala
val x = new Json2Spark(Json2Spark.file2String("src/test/resources/in-network-rates.json"), 
	defsLocation="definitions")

StructType(x.defs("in_network")).prettyJson
/*
String =
{
  "type" : "struct",
  "fields" : [ {
    "name" : "negotiation_arrangement",
    "type" : "string",
    "nullable" : true,
    "metadata" : {
      "description" : "",
      "path" : "#/definitions/in_network/properties/negotiation_arrangement"
    }
  }, {
    "name" : "name",
    "type" : "string",
    "nullable" : true,
    "metadata" : {
      "description" : "",
      "path" : "#/definitions/in_network/properties/name"
    }
  },
...
*/
```

#### Reference resolution 
Automatically resolving and translating references like below into a unified spark schema

```json
    "vegetables": {
      "type": "array",
      "items": { "$ref": "#/$defs/veggie" }
    }
```

```scala
val x = new Json2Spark(Json2Spark.file2String("src/test/resources/veggies.schema.json"))
x.convert2Spark.prettyJson
/*
String =
{
  "type" : "struct",
  "fields" : [ {
    "name" : "fruits",
    "type" : {
      "type" : "array",
      "elementType" : "string",
      "containsNull" : true
    },
    "nullable" : true,
    "metadata" : {
      "description" : "",
      "path" : "#/properties/fruits"
    }
  }, {
    "name" : "vegetables",
    "type" : {
      "type" : "array",
      "elementType" : {
        "type" : "struct",
        "fields" : [ {
          "name" : "veggieName",
          "type" : "string",
          "nullable" : true,
          "metadata" : {
            "description" : "The name of the vegetable.",
            "path" : "#/properties/vegetables/items/properties/veggieName"
          }
        }, {
          "name" : "veggieLike",
          "type" : "b...
*/	  
```

#### Circular Depdencies 
Some schemas have self references and extensions that result in an unlimited schema definition (stack overflow errors). These can be filtered out by specifying their path like below

```scala
val x = new Json2Spark(Json2Spark.file2String("src/test/resources/fhir.schema.json"), 
	circularReferences=Some(Seq("#/definitions/Extension", "#/definitions/Element", "#/definitions/Identifier", "#/definitions/Period","#/definitions/Reference")))
	
new StructType(x.defs("Patient").toArray).prettyJson
/*
String =
{
  "type" : "struct",
  "fields" : [ {
    "name" : "id",
    "type" : "string",
    "nullable" : true,
    "metadata" : {
      "description" : "Any combination of letters, numerals, \"-\" and \".\", with a length limit of 64 characters.  (This might be an integer, an unprefixed OID, UUID or any other identifier pattern that meets these constraints.)  Ids are case-insensitive.",
      "path" : "#/definitions/Patient/properties/id/$ref//#/definitions/id"
    }
  }, {
    "name" : "meta",
    "type" : {
      "type" : "struct",
      "fields" : [ {
        "name" : "id",
        "type" : "string",
        "nullable" : true,
        "metadata" : {
          "description" : "A sequence of Unicode characters",
          "path" : "#/definitions/Pati...
*/
```

#### Saving schema to a file for future use 
```scala
import java.io._

val fileName = ??? //add your location here 
val file = new FileWriter(new File(fileName))
file.write(x.convert2Spark.prettyJson)
file.close()
```
---

## ⚖️ License

This project is licensed under the **Apache License 2.0**.  
Original work forked from [Databricks Industry Solutions json2spark-schema](https://github.com/databricks-industry-solutions/json2spark-schema).  
