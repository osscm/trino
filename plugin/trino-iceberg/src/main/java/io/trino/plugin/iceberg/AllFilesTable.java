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
package io.trino.plugin.iceberg;

import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;

import static org.apache.iceberg.MetadataTableType.ALL_DATA_FILES;
import static org.apache.iceberg.MetadataTableUtils.createMetadataTableInstance;

public class AllFilesTable
        extends AbstractFilesTable
{
    public AllFilesTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable)
    {
        super(tableName, typeManager, icebergTable);
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        return new FixedPageSource(buildPages(getIcebergTable()));
    }

    @Override
    protected TableScan buildTableScan()
    {
        Table filesTable = createMetadataTableInstance(getIcebergTable(), ALL_DATA_FILES);

        TableScan tableScan = filesTable
                .newScan()
                .includeColumnStats();
        return tableScan;
    }
}
