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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.plugin.iceberg.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public abstract class AbstractFilesTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;

    public AbstractFilesTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        this.tableMetadata = tableMetadata(tableName, typeManager);
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    protected List<Page> buildPages(Table icebergTable)
    {
        PageListBuilder pagesBuilder = PageListBuilder.forTable(getTableMetadata());
        Map<Integer, Type> idToTypeMapping = getIcebergIdToTypeMapping(icebergTable.schema());
        TableScan tableScan = buildTableScan();

        tableScan.planFiles().forEach(fileScanTask -> {
            DataTask dataTask = (DataTask) fileScanTask;

            dataTask.rows().forEach(structLike -> addRow(pagesBuilder, idToTypeMapping, structLike));
        });

        return pagesBuilder.build();
    }

    protected abstract TableScan buildTableScan();

    protected Table getIcebergTable()
    {
        return icebergTable;
    }

    protected ConnectorTableMetadata tableMetadata(SchemaTableName tableName, TypeManager typeManager)
    {
        return new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("content", INTEGER))
                        .add(new ColumnMetadata("file_path", VARCHAR))
                        .add(new ColumnMetadata("file_format", VARCHAR))
                        .add(new ColumnMetadata("record_count", BIGINT))
                        .add(new ColumnMetadata("file_size_in_bytes", BIGINT))
                        .add(new ColumnMetadata("column_sizes", typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))))
                        .add(new ColumnMetadata("value_counts", typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))))
                        .add(new ColumnMetadata("null_value_counts", typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))))
                        .add(new ColumnMetadata("nan_value_counts", typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))))
                        .add(new ColumnMetadata("lower_bounds", typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()))))
                        .add(new ColumnMetadata("upper_bounds", typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()))))
                        .add(new ColumnMetadata("key_metadata", VARBINARY))
                        .add(new ColumnMetadata("split_offsets", new ArrayType(BIGINT)))
                        .add(new ColumnMetadata("equality_ids", new ArrayType(INTEGER)))
                        .build());
    }

    protected void addRow(PageListBuilder pagesBuilder, Map<Integer, Type> idToTypeMapping, StructLike structLike)
    {
        DataFile dataFile = (DataFile) structLike;

        pagesBuilder.beginRow();

        pagesBuilder.appendInteger(dataFile.content().id());
        pagesBuilder.appendVarchar(dataFile.path().toString());
        pagesBuilder.appendVarchar(dataFile.format().name());
        pagesBuilder.appendBigint(dataFile.recordCount());
        pagesBuilder.appendBigint(dataFile.fileSizeInBytes());
        if (checkNonNull(dataFile.columnSizes(), pagesBuilder)) {
            pagesBuilder.appendIntegerBigintMap(dataFile.columnSizes());
        }
        if (checkNonNull(dataFile.valueCounts(), pagesBuilder)) {
            pagesBuilder.appendIntegerBigintMap(dataFile.valueCounts());
        }
        if (checkNonNull(dataFile.nullValueCounts(), pagesBuilder)) {
            pagesBuilder.appendIntegerBigintMap(dataFile.nullValueCounts());
        }
        if (checkNonNull(dataFile.nanValueCounts(), pagesBuilder)) {
            pagesBuilder.appendIntegerBigintMap(dataFile.nanValueCounts());
        }
        if (checkNonNull(dataFile.lowerBounds(), pagesBuilder)) {
            pagesBuilder.appendIntegerVarcharMap(dataFile.lowerBounds().entrySet().stream()
                    .filter(entry -> idToTypeMapping.containsKey(entry.getKey()))
                    .collect(toImmutableMap(
                            Map.Entry<Integer, ByteBuffer>::getKey,
                            entry -> Transforms.identity(idToTypeMapping.get(entry.getKey())).toHumanString(
                                    Conversions.fromByteBuffer(idToTypeMapping.get(entry.getKey()), entry.getValue())))));
        }
        if (checkNonNull(dataFile.upperBounds(), pagesBuilder)) {
            pagesBuilder.appendIntegerVarcharMap(dataFile.upperBounds().entrySet().stream()
                    .filter(entry -> idToTypeMapping.containsKey(entry.getKey()))
                    .collect(toImmutableMap(
                            Map.Entry<Integer, ByteBuffer>::getKey,
                            entry -> Transforms.identity(idToTypeMapping.get(entry.getKey())).toHumanString(
                                    Conversions.fromByteBuffer(idToTypeMapping.get(entry.getKey()), entry.getValue())))));
        }
        if (checkNonNull(dataFile.keyMetadata(), pagesBuilder)) {
            pagesBuilder.appendVarbinary(Slices.wrappedBuffer(dataFile.keyMetadata()));
        }
        if (checkNonNull(dataFile.splitOffsets(), pagesBuilder)) {
            pagesBuilder.appendBigintArray(dataFile.splitOffsets());
        }
        if (checkNonNull(dataFile.equalityFieldIds(), pagesBuilder)) {
            pagesBuilder.appendIntegerArray(dataFile.equalityFieldIds());
        }
        pagesBuilder.endRow();
    }

    protected boolean checkNonNull(Object object, PageListBuilder pagesBuilder)
    {
        if (object == null) {
            pagesBuilder.appendNull();
            return false;
        }
        return true;
    }

    protected Map<Integer, Type> getIcebergIdToTypeMapping(Schema schema)
    {
        ImmutableMap.Builder<Integer, Type> icebergIdToTypeMapping = ImmutableMap.builder();
        for (Types.NestedField field : schema.columns()) {
            populateIcebergIdToTypeMapping(field, icebergIdToTypeMapping);
        }
        return icebergIdToTypeMapping.buildOrThrow();
    }

    private void populateIcebergIdToTypeMapping(Types.NestedField field, ImmutableMap.Builder<Integer, Type> icebergIdToTypeMapping)
    {
        Type type = field.type();
        icebergIdToTypeMapping.put(field.fieldId(), type);
        if (type instanceof Type.NestedType) {
            type.asNestedType().fields().forEach(child -> populateIcebergIdToTypeMapping(child, icebergIdToTypeMapping));
        }
    }
}
