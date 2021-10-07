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
package io.trino.plugin.h3;

import io.airlift.slice.Slices;
import io.trino.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestH3Plugin
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        functionAssertions.installPlugin(new H3Plugin());
    }

    @Test
    public void testGeoToH3Address()
    {
        assertFunction("geo_to_h3_address(45.0, 40.0, 15)", VARCHAR, "8f2d55c256ac883");
    }

    @Test
    public void testH3ToWkt()
    {
        assertFunction("h3_to_wkt('8f2d55c256ac883')", VARCHAR, Slices.utf8Slice("POLYGON ((39.99999168658859 45.00000521415798, 39.99999036498484 45.000000175041045, 39.99999598387472 44.9999965285094, 40.000002924368104 44.99999792109448, 40.00000424597252 45.000002960211184, 39.99999862708287 45.00000660674304))"));
    }
}
