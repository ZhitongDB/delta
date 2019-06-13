/*
 * Copyright 2019 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import java.io.File

import io.delta.DeltaTable
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

class DeleteSuiteBase extends QueryTest
  with SharedSQLContext
  with BeforeAndAfterEach {

  import testImplicits._

  var tempDir: File = _

  var deltaLog: DeltaLog = _

  private def tempPath = tempDir.getCanonicalPath

  private def getDeltaFileStmt(path: String) = s"SELECT key, value FROM delta.`$path`"

  private def readDeltaTable(path: String): DataFrame = {
    spark.read.format("delta").load(path)
  }

  override def beforeEach() {
    super.beforeEach()
    tempDir = Utils.createTempDir()
    deltaLog = DeltaLog.forTable(spark, new Path(tempPath))
  }

  override def afterEach() {
    try {
      Utils.deleteRecursively(tempDir)
      DeltaLog.clearCache()
    } finally {
      super.afterEach()
    }
  }

  protected def executeDelete(target: String, where: String = null): Unit = {
    val deltaTable: io.delta.DeltaTable = {
      var t: io.delta.DeltaTable = new DeltaTable(spark.table(target))
      t
    }
    if (where != null) {
      deltaTable.delete(where)
//      deltaTable.toDF.show()
    } else {
      deltaTable.delete()
    }
  }

  private def append(df: DataFrame, partitionBy: Seq[String] = Nil): Unit = {
    val writer = df.write.format("delta").mode("append")
    if (partitionBy.nonEmpty) {
      writer.partitionBy(partitionBy: _*)
    }
    writer.save(deltaLog.dataPath.toString)
  }

  private def checkDelete(
    condition: Option[String],
    expectedResults: Seq[Row],
    tableName: Option[String] = None): Unit = {
    spark.read.format("delta").load(tempPath).createOrReplaceTempView("deltaTable")
    executeDelete(target = "deltaTable", where = condition.orNull)
    checkAnswer(readDeltaTable(tempPath), expectedResults)
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic case - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkDelete(condition = None, Nil)
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic case - delete from a Delta table - Partition=$isPartitioned") {
      withTable("delta_table") {

        val partitions = if (isPartitioned) "key" :: Nil else Nil
        val input = Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value")
        append(input, partitions)

        checkDelete(Some("value = 4 and key = 3"),
          Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
        checkDelete(Some("value = 4 and key = 1"),
          Row(2, 2) :: Row(1, 1) :: Row(0, 3) :: Nil)
        checkDelete(Some("value = 2 or key = 1"),
          Row(0, 3) :: Nil)
        checkDelete(Some("key = 0 or value = 99"),
          Nil)
      }
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic key columns - Partition=$isPartitioned") {
      val input = Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value")
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(input, partitions)

      checkDelete(Some("key > 2"), Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
      checkDelete(Some("key < 2"), Row(2, 2) :: Nil)
      checkDelete(Some("key = 2"), Nil)
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"where key columns - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkDelete(Some("key = 1"), Row(2, 2) :: Row(0, 3) :: Nil)
      checkDelete(Some("key = 2"), Row(0, 3) :: Nil)
      checkDelete(Some("key = 0"), Nil)
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"where data columns - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkDelete(Some("value <= 2"), Row(1, 4) :: Row(0, 3) :: Nil)
      checkDelete(Some("value = 3"), Row(1, 4) :: Nil)
      checkDelete(Some("value != 0"), Nil)
    }
  }

  test("where data columns and partition columns") {
    val input = Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value")
    append(input, Seq("key"))

    checkDelete(Some("value = 4 and key = 3"),
      Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
    checkDelete(Some("value = 4 and key = 1"),
      Row(2, 2) :: Row(1, 1) :: Row(0, 3) :: Nil)
    checkDelete(Some("value = 2 or key = 1"),
      Row(0, 3) :: Nil)
    checkDelete(Some("key = 0 or value = 99"),
      Nil)
  }

  Seq(true, false).foreach { skippingEnabled =>
    Seq(true, false).foreach { isPartitioned =>
      test(s"data and partition columns - Partition=$isPartitioned Skipping=$skippingEnabled") {
        withSQLConf(DeltaSQLConf.DELTA_STATS_SKIPPING.key -> skippingEnabled.toString) {
          val partitions = if (isPartitioned) "key" :: Nil else Nil
          val input = Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value")
          append(input, partitions)

          checkDelete(Some("value = 4 and key = 3"),
            Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
          checkDelete(Some("value = 4 and key = 1"),
            Row(2, 2) :: Row(1, 1) :: Row(0, 3) :: Nil)
          checkDelete(Some("value = 2 or key = 1"),
            Row(0, 3) :: Nil)
          checkDelete(Some("key = 0 or value = 99"),
            Nil)
        }
      }
    }
  }

  test("Negative case - non-Delta target") {
    withTable("nondeltaTab") {
      Seq((1, 1), (0, 3), (1, 5)).toDF("key1", "value")
        .write.format("parquet").saveAsTable("nonDeltaTab")

      val e = intercept[AnalysisException] {
        executeDelete("nonDeltaTab")
      }.getMessage
      assert(e.contains("DELETE destination only supports Delta sources"))
    }
  }

  test("Negative case - non-deterministic condition") {
    append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))
    val e = intercept[AnalysisException] {
//      executeDelete(target = s"delta.`$tempPath`", where = "rand() > 0.5")
      spark.read.format("delta").load(tempPath).createOrReplaceTempView("deltaTable")
      executeDelete(target = "deltaTable", where = "rand() > 0.5")
    }.getMessage
    assert(e.contains("nondeterministic expressions are only allowed in"))
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"condition having current_date - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(
        Seq((java.sql.Date.valueOf("1969-12-31"), 2),
          (java.sql.Date.valueOf("2099-12-31"), 4))
          .toDF("key", "value"), partitions)

      checkDelete(Some("CURRENT_DATE > key"),
        Row(java.sql.Date.valueOf("2099-12-31"), 4) :: Nil)
      checkDelete(Some("CURRENT_DATE <= key"), Nil)
    }
  }

  test("condition having current_timestamp - Partition by Timestamp") {
    append(
      Seq((java.sql.Timestamp.valueOf("2012-12-31 16:00:10.011"), 2),
        (java.sql.Timestamp.valueOf("2099-12-31 16:00:10.011"), 4))
        .toDF("key", "value"), Seq("key"))

    checkDelete(Some("CURRENT_TIMESTAMP > key"),
      Row(java.sql.Timestamp.valueOf("2099-12-31 16:00:10.011"), 4) :: Nil)
    checkDelete(Some("CURRENT_TIMESTAMP <= key"), Nil)
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"foldable condition - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      val allRows = Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil

      checkDelete(Some("false"), allRows)
      checkDelete(Some("1 <> 1"), allRows)
      checkDelete(Some("1 > null"), allRows)
      checkDelete(Some("true"), Nil)
      checkDelete(Some("1 = 1"), Nil)
    }
  }

//  test("Negative case - DELETE the child directory") {
//    append(Seq((2, 2), (3, 2)).toDF("key", "value"), partitionBy = "key" :: Nil)
//    val e = intercept[AnalysisException] {
//      executeDelete(target = s"delta.`$tempPath/key=2`", where = "value = 2")
//    }.getMessage
//    assert(e.contains("Expect a full scan of Delta sources, but found the partial scan"))
//  }

//  test("explain") {
//    append(Seq((2, 2)).toDF("key", "value"))
//    val df = sql(s"EXPLAIN DELETE FROM delta.`$tempPath` WHERE key = 2")
//    val outputs = df.collect().map(_.mkString).mkString
//    assert(outputs.contains("Delta"))
//    assert(!outputs.contains("index") && !outputs.contains("ActionLog"))
//    // no change should be made by explain
//    checkAnswer(sql(getDeltaFileStmt(tempPath)), Row(2, 2))
//  }

//  test("delete cached table") {
//    withTable("deltaTable") {
//      Seq((2, 2), (1, 4)).toDF("key", "value").write.format("delta").saveAsTable("deltaTable")
//
//      spark.table("deltaTable").cache()
//      spark.table("deltaTable").collect()
//      executeDelete(target = "deltaTable", where = "key = 2")
//      checkAnswer(spark.table("deltaTable"), Row(1, 4) :: Nil)
//    }
//  }

  test("SC-12232: should not delete the rows where condition evaluates to null") {
    append(Seq(("a", null), ("b", null), ("c", "v"), ("d", "vv")).toDF("key", "value").coalesce(1))

    // "null = null" evaluates to null
    checkDelete(Some("value = null"),
      Row("a", null) :: Row("b", null) :: Row("c", "v") :: Row("d", "vv") :: Nil)

    // these expressions evaluate to null when value is null
    checkDelete(Some("value = 'v'"),
      Row("a", null) :: Row("b", null) :: Row("d", "vv") :: Nil)
    checkDelete(Some("value <> 'v'"),
      Row("a", null) :: Row("b", null) :: Nil)
  }

  test("SC-12232: delete rows with null values using isNull") {
    append(Seq(("a", null), ("b", null), ("c", "v"), ("d", "vv")).toDF("key", "value").coalesce(1))

    // when value is null, this expression evaluates to true
    checkDelete(Some("value is null"),
      Row("c", "v") :: Row("d", "vv") :: Nil)
  }

  test("SC-12232: delete rows with null values using EqualNullSafe") {
    append(Seq(("a", null), ("b", null), ("c", "v"), ("d", "vv")).toDF("key", "value").coalesce(1))

    // when value is null, this expression evaluates to true
    checkDelete(Some("value <=> null"),
      Row("c", "v") :: Row("d", "vv") :: Nil)
  }

//  test("SC-12655: add file search in rewritten files should work with relativization fixes") {
//    withTempDir { dir =>
//      setupTableWithAbsolutePath(spark, dir, spark.range(10).toDF())
//
//      executeDelete(target = s"delta.`$dir`", where = "id >= 5")
//      checkAnswer(
//        spark.read.format("delta").load(dir.getCanonicalPath),
//        spark.range(5).toDF()
//      )
//    }
//  }
//
//  test("SC-13255: Allow non-unique filenames") {
//    withTempDir { tempDir =>
//      val dir = new File(tempDir, "foo %+bar").getCanonicalFile
//      asSuper {
//        val df = spark.range(10).toDF("id").withColumn("part", 'id % 2)
//        setupTableWithNonUniqueFilenames(spark, dir, df, "part" :: Nil)
//
//        executeDelete(target = s"delta.`$dir`", where = "id >= 5")
//        checkAnswer(
//          spark.read.format("delta").load(dir.getCanonicalPath),
//          spark.range(5).toDF().toDF("id").withColumn("part", 'id % 2)
//        )
//      }
//    }
//  }
}
