package io.github.saidbouras.spark.json.schema

import org.scalatest.funsuite.AnyFunSuite

class JsonCyclesTest extends AnyFunSuite {

  test("test json with no cycle") {
    val model = Json2Spark.jsonContainsCycle("src/test/resources/models/model.schema.json")
    assert(!model)
  }

  test("test json with cycle at depth 2") {
    val twoDepth = Json2Spark.jsonContainsCycle("src/test/resources/cycles/depth-two.schema.json")
    assert(twoDepth)
  }

  test("test json with cycle at depth 5") {
    val result = Json2Spark.jsonContainsCycle("src/test/resources/cycles/depth-five.schema.json")
    assert(result)
  }
  test("test json with cycle at more than 5 (deep cycle)") {
    val result = Json2Spark.jsonContainsCycle("src/test/resources/cycles/depth-deep.schema.json")
    assert(result)
  }
}