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
package io.trino.plugin.iceberg.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergSplit;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.sizeOf;

public class AggregateIcebergSplit
        extends IcebergSplit
{
    private final long totalCount;

    @JsonCreator
    public AggregateIcebergSplit(
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("fileRecordCount") long fileRecordCount,
            @JsonProperty("fileFormat") IcebergFileFormat fileFormat,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("partitionSpecJson") String partitionSpecJson,
            @JsonProperty("partitionDataJson") String partitionDataJson,
            @JsonProperty("deletes") List<DeleteFile> deletes,
            @JsonProperty("splitWeight") SplitWeight splitWeight,
            @JsonProperty("totalCount") long totalCount)
    {
        super(path, start, length, fileSize, fileRecordCount, fileFormat, addresses, partitionSpecJson, partitionDataJson, deletes, splitWeight);
        this.totalCount = totalCount;
    }

    @JsonProperty
    public long getTotalCount()
    {
        return totalCount;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("totalCount", totalCount)
                .buildOrThrow();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        final AtomicLong columnsToMaxSize = new AtomicLong(0L);
        final AtomicLong columnsToMinSize = new AtomicLong(0L);

        return super.getRetainedSizeInBytes() +
                +sizeOf(OptionalLong.of(totalCount))
                + columnsToMaxSize.longValue()
                + columnsToMinSize.longValue();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(totalCount)
                .toString();
    }
}
