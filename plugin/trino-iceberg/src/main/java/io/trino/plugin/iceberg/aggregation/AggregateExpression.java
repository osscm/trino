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
import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.ColumnIdentity;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataColumns.COMMIT_SNAPSHOT_ID;

public class AggregateExpression
{
    private final String function;
    private final String argument;
    private final boolean returnNullOnEmptyGroup;
    // did not find a cleaner way to find a id, Iceberg has COMMIT_SNAPSHOT_ID is the start of the synthetic columns, and it goes till FILE_PATH which Integer.MAX_VALUE
    // which is the largest ID. Therefore chosen the COMMIT_SNAPSHOT_ID - 1
    public static final Integer COUNT_AGGREGATE_COLUMN_ID = COMMIT_SNAPSHOT_ID.fieldId() - 1;

    @JsonCreator
    public AggregateExpression(@JsonProperty String function, @JsonProperty String argument, @JsonProperty boolean returnNullOnEmptyGroup)
    {
        this.function = requireNonNull(function, "function is null");
        this.argument = requireNonNull(argument, "argument is null");
        this.returnNullOnEmptyGroup = returnNullOnEmptyGroup;
    }

    @JsonProperty
    public String getFunction()
    {
        return function;
    }

    @JsonProperty
    public String getArgument()
    {
        return argument;
    }

    @JsonProperty
    public boolean isReturnNullOnEmptyGroup()
    {
        return returnNullOnEmptyGroup;
    }

    public ColumnIdentity toColumnIdentity(Integer columnId)
    {
        return new ColumnIdentity(columnId, format("%s(%s)", function, argument), ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of());
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof AggregateExpression)) {
            return false;
        }
        AggregateExpression that = (AggregateExpression) other;
        return that.function.equals(function) &&
                that.argument.equals(argument) &&
                that.returnNullOnEmptyGroup == returnNullOnEmptyGroup;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(function, argument, returnNullOnEmptyGroup);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("function", function)
                .add("argument", argument)
                .add("returnNullOnEmptyGroup", returnNullOnEmptyGroup)
                .toString();
    }
}
