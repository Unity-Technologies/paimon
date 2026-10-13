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

package org.apache.paimon.operation;

import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.Identifier;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Lock}. */
public class LockTest {

    @Test
    public void testCatalogLockName() throws Exception {
        List<String> locked = new ArrayList<>();
        CatalogLock catalogLock =
                new CatalogLock() {

                    @Override
                    public <T> T runWithLock(String database, String table, Callable<T> callable)
                            throws Exception {
                        locked.add(database + "." + table);
                        return callable.call();
                    }

                    @Override
                    public void close() {}
                };

        for (String object : new String[] {"t", "t$branch_b1", "t$branch_main"}) {
            Lock.fromCatalog(catalogLock, Identifier.create("db", object)).runWithLock(() -> null);
        }
        // the main branch is locked by the table name
        assertThat(locked).containsExactly("db.t", "db.t$branch_b1", "db.t");
    }
}
