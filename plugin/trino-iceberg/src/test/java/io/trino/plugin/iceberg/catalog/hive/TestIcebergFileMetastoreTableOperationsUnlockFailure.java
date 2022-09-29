/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg.catalog.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.metastore.AcidTransactionOwner;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.thrift.InMemoryThriftMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.plugin.iceberg.TestingIcebergConnectorFactory;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

@Test(singleThreaded = true)
public class TestIcebergFileMetastoreTableOperationsUnlockFailure
        extends AbstractTestQueryFramework
{
    private static final String ICEBERG_CATALOG = "iceberg";
    private static final String SCHEMA_NAME = "test_schema";
    private File baseDir;

    @Override
    protected LocalQueryRunner createQueryRunner()
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(SCHEMA_NAME)
                .build();

        try {
            baseDir = Files.createTempDirectory(null).toFile();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        LocalQueryRunner queryRunner = LocalQueryRunner.create(session);

        ThriftMetastore thriftMetastore = createMetastoreWithUnlockFailure();
        TestingIcebergHiveMetastoreCatalogModule testModule = new TestingIcebergHiveMetastoreCatalogModule(thriftMetastore);
        HiveMetastore metastore = testModule.getHiveMetastore();

        queryRunner.createCatalog(
                ICEBERG_CATALOG,
                new TestingIcebergConnectorFactory(Optional.of(testModule), Optional.empty(), EMPTY_MODULE),
                ImmutableMap.of());

        Database database = Database.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
        metastore.createDatabase(database);

        return queryRunner;
    }

    private InMemoryThriftMetastore createMetastoreWithUnlockFailure() {
        return new InMemoryThriftMetastore(new File(baseDir + "/metastore"), new ThriftMetastoreConfig()) {
            @Override
            public long acquireTableExclusiveLock(AcidTransactionOwner transactionOwner, String queryId, String dbName, String tableName) {
                // returning dummy lock
                return 100;
            }

            @Override
            public void releaseTableLock(long lockId) {
                throw new RuntimeException("Unlock failed!");
            }

            @Override
            public synchronized void createTable(org.apache.hadoop.hive.metastore.api.Table table) {
                // InMemoryThriftMetastore throws an exception if the table has any privileges set
                table.setPrivileges(null);
                super.createTable(table);
            }
        };
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws Exception
    {
        if (baseDir != null) {
            deleteRecursively(baseDir.toPath(), ALLOW_INSECURE);
        }
    }

    @Test
    public void testUnlockFailureDoesNotCorruptTheTableMetadata()
    {
        String tableName = "test_unlock_failure";
        getQueryRunner().execute(format("CREATE TABLE %s (a_varchar) AS VALUES ('Trino')", tableName));
        getQueryRunner().execute("INSERT INTO " + tableName + " VALUES 'rocks'");
        assertQuery("SELECT * FROM " + tableName, "VALUES 'Trino', 'rocks'");
    }
}
