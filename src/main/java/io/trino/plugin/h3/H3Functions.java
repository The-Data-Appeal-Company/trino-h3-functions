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

import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.io.IOException;
import java.util.List;

public final class H3Functions
{
    private H3Functions()
    {
    }

    @ScalarFunction("geo_to_h3_address")
    @Description("Returns h3 address from lat, lng and resolution level")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice geoToH3Address(
            @SqlType(StandardTypes.DOUBLE) double lat,
            @SqlType(StandardTypes.DOUBLE) double lng,
            @SqlType(StandardTypes.INTEGER) long res)
    {
        H3Core h3;
        try {
            h3 = H3Core.newInstance();
        }
        catch (IOException e) {
            throw new IllegalStateException(e.getMessage());
        }
        return Slices.utf8Slice(h3.geoToH3Address(lat, lng, (int) res));
    }

    @ScalarFunction("h3_to_wkt")
    @Description("Returns wkt from h3 hex")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice h3ToWkt(@SqlType(StandardTypes.VARCHAR) Slice hex)
    {
        H3Core h3;
        try {
            h3 = H3Core.newInstance();
        }
        catch (IOException e) {
            throw new IllegalStateException(e.getMessage());
        }
        List<GeoCoord> geoCoords = h3.h3ToGeoBoundary(hex.toStringUtf8());
        if (geoCoords.isEmpty()) {
            throw new IllegalStateException("invalid hex");
        }
        StringBuilder polygon = new StringBuilder("POLYGON ((");
        for (GeoCoord geoCoord : geoCoords) {
            polygon.append(geoCoord.lng);
            polygon.append(" ");
            polygon.append(geoCoord.lat);
            polygon.append(", ");
        }
        int l = polygon.length();
        polygon.replace(l - 2, l, "))");
        return Slices.utf8Slice(polygon.toString());
    }
}
