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

import org.testng.annotations.Test;

import static io.trino.plugin.iceberg.IcebergUtil.fromColumnToIdentifier;
import static io.trino.plugin.iceberg.IcebergUtil.fromIdentifierToColumn;
import static io.trino.testing.assertions.Assert.assertEquals;

public class TestIcebergUtil
{
    @Test
    public void testFromIdentifierToColumn()
    {
        assertEquals(fromIdentifierToColumn("test"), "test");
        assertEquals(fromIdentifierToColumn("TEST"), "test");
        assertEquals(fromIdentifierToColumn("\" test\""), " test");
        assertEquals(fromIdentifierToColumn("\"20days\""), "20days");
        assertEquals(fromIdentifierToColumn("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\""), "\"another\" \"quoted\" \"field\"");
    }

    @Test
    public void testFromColumnToIdentifier()
    {
        assertEquals(fromColumnToIdentifier("test"), "test");
        assertEquals(fromColumnToIdentifier(" test"), "\" test\"");
        assertEquals(fromColumnToIdentifier("20days"), "\"20days\"");
        assertEquals(fromColumnToIdentifier("\"another\" \"quoted\" \"field\""), "\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\"");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Uppercase characters .* are not supported.")
    public void testUppercaseIdentifierFails()
    {
        fromIdentifierToColumn("\"UppErCaSe\"");
    }
}
