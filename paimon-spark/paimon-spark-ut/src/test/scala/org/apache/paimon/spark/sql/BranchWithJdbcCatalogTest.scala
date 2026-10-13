/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.sql

import org.apache.paimon.catalog.DelegateCatalog
import org.apache.paimon.jdbc.JdbcCatalog
import org.apache.paimon.spark.PaimonSparkTestWithJdbcCatalogBase

import org.apache.spark.sql.Row

class BranchWithJdbcCatalogTest extends PaimonSparkTestWithJdbcCatalogBase {

  import testImplicits._

  test("Branch with JDBC catalog: catalog is a JDBC catalog") {
    assert(DelegateCatalog.rootCatalog(paimonCatalog).isInstanceOf[JdbcCatalog])
  }

  test("Branch with JDBC catalog: create, list, rename and delete branches") {
    createTableWithBranch()
    sql("CALL sys.create_branch(table => 'test.T', branch => 'empty')")
    checkAnswer(
      sql("SELECT branch_name FROM `T$branches` ORDER BY branch_name"),
      Row("b1") :: Row("empty") :: Nil)
    checkAnswer(sql("SELECT * FROM `T$branch_empty`"), Nil)

    sql("CALL sys.rename_branch(table => 'test.T', from_branch => 'b1', to_branch => 'b2')")
    checkAnswer(
      sql("SELECT branch_name FROM `T$branches` ORDER BY branch_name"),
      Row("b2") :: Row("empty") :: Nil)
    checkAnswer(sql("SELECT * FROM `T$branch_b2` ORDER BY a"), Row(1, "a") :: Row(2, "b") :: Nil)
    assert(intercept[Exception](sql("SELECT * FROM `T$branch_b1`")).getMessage.contains("b1"))

    sql("CALL sys.delete_branch(table => 'test.T', branch => 'b2,empty')")
    checkAnswer(sql("SELECT branch_name FROM `T$branches`"), Nil)
    assert(intercept[Exception](sql("SELECT * FROM `T$branch_b2`")).getMessage.contains("b2"))
    checkAnswer(sql("SELECT * FROM T ORDER BY a"), Row(1, "a") :: Row(2, "b") :: Nil)
  }

  test("Branch with JDBC catalog: read and write a branch with SQL") {
    createTableWithBranch()
    sql("INSERT INTO `T$branch_b1` VALUES (3, 'c')")
    sql("INSERT INTO T VALUES (4, 'd')")

    checkAnswer(
      sql("SELECT * FROM `T$branch_b1` ORDER BY a"),
      Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)
    checkAnswer(sql("SELECT * FROM T ORDER BY a"), Row(1, "a") :: Row(2, "b") :: Row(4, "d") :: Nil)
    checkAnswer(sql("SELECT snapshot_id FROM `T$branch_b1$snapshots`"), Row(1) :: Row(2) :: Nil)
  }

  test("Branch with JDBC catalog: read and write a branch with DataFrame") {
    createTableWithBranch()
    Seq((3, "c")).toDF("a", "b").writeTo("paimon.test.`T$branch_b1`").append()
    Seq((4, "d"))
      .toDF("a", "b")
      .write
      .format("paimon")
      .mode("append")
      .saveAsTable("paimon.test.`T$branch_b1`")

    checkAnswer(
      spark.table("paimon.test.`T$branch_b1`").orderBy("a"),
      Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Row(4, "d") :: Nil)
    checkAnswer(spark.table("paimon.test.T").orderBy("a"), Row(1, "a") :: Row(2, "b") :: Nil)
  }

  test("Branch with JDBC catalog: alter a branch") {
    createTableWithBranch()
    sql("ALTER TABLE `T$branch_b1` ADD COLUMNS (c INT)")
    sql("ALTER TABLE `T$branch_b1` SET TBLPROPERTIES ('k' = 'v')")
    sql("INSERT INTO `T$branch_b1` VALUES (3, 'c', 30)")

    checkAnswer(
      sql("SELECT * FROM `T$branch_b1` ORDER BY a"),
      Row(1, "a", null) :: Row(2, "b", null) :: Row(3, "c", 30) :: Nil)
    assert(loadTable("T$branch_b1").options().get("k") == "v")

    // main is not changed
    checkAnswer(sql("SELECT * FROM T ORDER BY a"), Row(1, "a") :: Row(2, "b") :: Nil)
    assert(!loadTable("T").options().containsKey("k"))
  }

  test("Branch with JDBC catalog: merge schema when writing a branch") {
    createTableWithBranch()
    withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
      sql("INSERT INTO `T$branch_b1` BY NAME SELECT 3 AS a, 'c' AS b, 30 AS c")
    }
    Seq((4, "d", 40, 4.5))
      .toDF("a", "b", "c", "d")
      .write
      .format("paimon")
      .mode("append")
      .option("write.merge-schema", "true")
      .saveAsTable("paimon.test.`T$branch_b1`")

    // merging the schema does not refresh the cached table, see WriteMergeSchemaTest
    sql("REFRESH TABLE `T$branch_b1`")
    checkAnswer(
      sql("SELECT * FROM `T$branch_b1` ORDER BY a"),
      Row(1, "a", null, null) :: Row(2, "b", null, null) :: Row(3, "c", 30, null) ::
        Row(4, "d", 40, 4.5) :: Nil
    )

    // main is not changed
    checkAnswer(sql("SELECT * FROM T ORDER BY a"), Row(1, "a") :: Row(2, "b") :: Nil)
  }

  /** Creates table T with two rows, and branch b1 from tag base on that data. */
  private def createTableWithBranch(): Unit = {
    sql("CREATE TABLE T (a INT, b STRING) TBLPROPERTIES ('primary-key' = 'a', 'bucket' = '1')")
    sql("INSERT INTO T VALUES (1, 'a'), (2, 'b')")
    sql("CALL sys.create_tag(table => 'test.T', tag => 'base')")
    sql("CALL sys.create_branch(table => 'test.T', branch => 'b1', tag => 'base')")
  }
}
