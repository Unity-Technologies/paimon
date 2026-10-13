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

package org.apache.paimon.spark

import org.apache.paimon.jdbc.{JdbcCatalogFactory, JdbcCatalogLockFactory}

import org.apache.spark.SparkConf
import org.apache.spark.sql.paimon.Utils

import java.io.File

/** Base for tests that use a [[SparkCatalog]] backed by a JDBC catalog with the JDBC lock. */
class PaimonSparkTestWithJdbcCatalogBase extends PaimonSparkTestBase {

  // a file database, because tables and locks open their own connections
  protected lazy val jdbcDBDir: File = Utils.createTempDir

  override protected def sparkConf: SparkConf = {
    val uri = s"jdbc:sqlite:${jdbcDBDir.getCanonicalPath}/catalog.sqlite?busy_timeout=60000"
    super.sparkConf
      .set("spark.sql.catalog.paimon.metastore", JdbcCatalogFactory.IDENTIFIER)
      .set("spark.sql.catalog.paimon.uri", uri)
      .set("spark.sql.catalog.paimon.jdbc.username", "user")
      .set("spark.sql.catalog.paimon.jdbc.password", "password")
      .set("spark.sql.catalog.paimon.lock.enabled", "true")
      .set("spark.sql.catalog.paimon.lock.type", JdbcCatalogLockFactory.IDENTIFIER)
  }
}
