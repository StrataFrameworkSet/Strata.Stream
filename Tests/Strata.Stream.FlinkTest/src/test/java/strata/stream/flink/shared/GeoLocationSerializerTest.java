/// ///////////////////////////////////////////////////////////////////////////
// GeoLocationSerializerTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import strata.foundation.core.value.GeoLocation;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("CommitStage")
public
class GeoLocationSerializerTest
{
    @ParameterizedTest
    @MethodSource("provideGeoLocations")
    public void
    testSerializeDeserialize(GeoLocation expected)
    {
        Kryo                    kryo = new Kryo();
        Output                  output = new Output(1024,1024*1024);
        Input                   input = new Input(output.getBuffer());
        GeoLocationSerializer   serializer = new GeoLocationSerializer();
        GeoLocation             actual = null;

        kryo.register(GeoLocation.class,serializer);
        serializer.write(kryo,output,expected);
        output.flush();
        actual = serializer.read(kryo,input,GeoLocation.class);

        assertEquals(expected,actual);
    }

    public static Stream<Arguments>
    provideGeoLocations()
    {
        return Stream.of(
            // Major world cities
            Arguments.of(GeoLocation.of(40.7128, -74.0060)), // New York City
            Arguments.of(GeoLocation.of(51.5074, -0.1278)),  // London
            Arguments.of(GeoLocation.of(48.8566, 2.3522)),   // Paris
            Arguments.of(GeoLocation.of(35.6762, 139.6503)), // Tokyo
            Arguments.of(GeoLocation.of(-33.8688, 151.2093)), // Sydney

            // US major cities
            Arguments.of(GeoLocation.of(34.0522, -118.2437)), // Los Angeles
            Arguments.of(GeoLocation.of(41.8781, -87.6298)),  // Chicago
            Arguments.of(GeoLocation.of(29.7604, -95.3698)),  // Houston
            Arguments.of(GeoLocation.of(33.4484, -112.0740)), // Phoenix
            Arguments.of(GeoLocation.of(39.7392, -104.9903)), // Denver

            // European cities
            Arguments.of(GeoLocation.of(52.5200, 13.4050)),   // Berlin
            Arguments.of(GeoLocation.of(41.9028, 12.4964)),   // Rome
            Arguments.of(GeoLocation.of(40.4168, -3.7038)),   // Madrid
            Arguments.of(GeoLocation.of(59.9311, 10.7583)),   // Oslo
            Arguments.of(GeoLocation.of(55.6761, 12.5683)),   // Copenhagen

            // Asian cities
            Arguments.of(GeoLocation.of(39.9042, 116.4074)),  // Beijing
            Arguments.of(GeoLocation.of(31.2304, 121.4737)), // Shanghai
            Arguments.of(GeoLocation.of(19.0760, 72.8777)),   // Mumbai
            Arguments.of(GeoLocation.of(1.3521, 103.8198)),   // Singapore
            Arguments.of(GeoLocation.of(37.5665, 126.9780)), // Seoul

            // Southern hemisphere locations
            Arguments.of(GeoLocation.of(-34.6037, -58.3816)), // Buenos Aires
            Arguments.of(GeoLocation.of(-22.9068, -43.1729)), // Rio de Janeiro
            Arguments.of(GeoLocation.of(-33.9249, 18.4241)),  // Cape Town
            Arguments.of(GeoLocation.of(-37.8136, 144.9631)), // Melbourne
            Arguments.of(GeoLocation.of(-36.8485, 174.7633)), // Auckland

            // Extreme coordinates
            Arguments.of(GeoLocation.of(90.0000, 0.0000)),    // North Pole
            Arguments.of(GeoLocation.of(-90.0000, 0.0000)),   // South Pole
            Arguments.of(GeoLocation.of(0.0000, 0.0000)),     // Equator/Prime Meridian
            Arguments.of(GeoLocation.of(0.0000, 180.0000)),   // Equator/International Date Line
            Arguments.of(GeoLocation.of(0.0000, -180.0000)),  // Equator/International Date Line (West)

            // High precision coordinates
            Arguments.of(GeoLocation.of(40.748817, -73.985428)), // Empire State Building
            Arguments.of(GeoLocation.of(48.858844, 2.294351)),   // Eiffel Tower
            Arguments.of(GeoLocation.of(51.499998, -0.124870)),  // Big Ben
            Arguments.of(GeoLocation.of(55.752121, 37.617664)),  // Red Square
            Arguments.of(GeoLocation.of(41.890251, 12.492373)),  // Colosseum

            // Island nations and remote locations
            Arguments.of(GeoLocation.of(21.3099, -157.8581)),  // Honolulu
            Arguments.of(GeoLocation.of(64.1466, -21.9426)),   // Reykjavik
            Arguments.of(GeoLocation.of(-8.5069, 179.1945)),   // Tuvalu
            Arguments.of(GeoLocation.of(7.3697, 134.4827)),    // Palau
            Arguments.of(GeoLocation.of(-54.4814, -36.1776)), // South Georgia Island

            // Desert and extreme climate locations
            Arguments.of(GeoLocation.of(23.5880, 58.3829)),    // Muscat, Oman
            Arguments.of(GeoLocation.of(24.4539, 54.3773)),    // Abu Dhabi
            Arguments.of(GeoLocation.of(-22.9576, -43.2096)), // Rio (alternate)
            Arguments.of(GeoLocation.of(71.0486, -8.2127)),    // Longyearbyen, Svalbard
            Arguments.of(GeoLocation.of(-77.8419, 166.6863)),  // McMurdo Station, Antarctica

            // Boundary testing coordinates
            Arguments.of(GeoLocation.of(89.9999, 179.9999)),   // Near North Pole, near date line
            Arguments.of(GeoLocation.of(-89.9999, -179.9999)), // Near South Pole, near date line
            Arguments.of(GeoLocation.of(0.0001, 0.0001)),      // Very close to origin
            Arguments.of(GeoLocation.of(-0.0001, -0.0001)),    // Very close to origin (negative)

            // Scientific stations and research facilities
            Arguments.of(GeoLocation.of(82.5018, -82.1111)),   // Alert, Canada (northernmost settlement)
            Arguments.of(GeoLocation.of(-54.8019, -68.3030)), // Ushuaia, Argentina (southernmost city)
            Arguments.of(GeoLocation.of(70.2204, -148.4594)), // Prudhoe Bay, Alaska
            Arguments.of(GeoLocation.of(-78.4636, 106.8370)),  // Vostok Station, Antarctica

            // Zero coordinate edge cases
            Arguments.of(GeoLocation.of(0.0, 0.0)),            // Origin
            Arguments.of(GeoLocation.of(45.0, 90.0)),          // Whole number coordinates
            Arguments.of(GeoLocation.of(-45.0, -90.0)),        // Negative whole numbers
            Arguments.of(GeoLocation.of(0.0, 1.0)),            // Simple decimal coordinates
            Arguments.of(GeoLocation.of(1.0, 0.0))             // Simple decimal coordinates (reversed)
        );
    }
}

//////////////////////////////////////////////////////////////////////////////
