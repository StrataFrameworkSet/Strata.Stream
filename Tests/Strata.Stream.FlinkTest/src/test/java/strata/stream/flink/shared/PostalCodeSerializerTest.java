/// ///////////////////////////////////////////////////////////////////////////
// PostalCodeSerializerTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import strata.foundation.core.value.PostalCode;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("CommitStage")
public
class PostalCodeSerializerTest
{
    @ParameterizedTest
    @MethodSource("providePostalCodes")
    public void
    testSerializeDeserialize(PostalCode expected)
    {
        Kryo                    kryo = new Kryo();
        Output                  output = new Output(1024,1024*1024);
        Input                   input = new Input(output.getBuffer());
        PostalCodeSerializer    serializer = new PostalCodeSerializer();
        PostalCode              actual = null;

        kryo.register(PostalCode.class,serializer);
        serializer.write(kryo,output,expected);
        output.flush();
        actual = serializer.read(kryo,input,PostalCode.class);

        assertEquals(expected,actual);
    }

    public static Stream<Arguments>
    providePostalCodes()
    {
        return Stream.of(
            // US ZIP codes (5 digit)
            Arguments.of(PostalCode.of("10001")), // New York, NY
            Arguments.of(PostalCode.of("90210")), // Beverly Hills, CA
            Arguments.of(PostalCode.of("60601")), // Chicago, IL
            Arguments.of(PostalCode.of("77001")), // Houston, TX
            Arguments.of(PostalCode.of("85001")), // Phoenix, AZ

            // US ZIP+4 codes (9 digit)
            Arguments.of(PostalCode.of("10001-1234")), // New York with extension
            Arguments.of(PostalCode.of("90210-5678")), // Beverly Hills with extension
            Arguments.of(PostalCode.of("60601-9012")), // Chicago with extension
            Arguments.of(PostalCode.of("77001-3456")), // Houston with extension
            Arguments.of(PostalCode.of("85001-7890")), // Phoenix with extension

            // Canadian postal codes
            Arguments.of(PostalCode.of("K1A 0A6")), // Ottawa, ON
            Arguments.of(PostalCode.of("M5H 2N2")), // Toronto, ON
            Arguments.of(PostalCode.of("H3A 0G4")), // Montreal, QC
            Arguments.of(PostalCode.of("V6B 2W9")), // Vancouver, BC
            Arguments.of(PostalCode.of("T2P 2M5")), // Calgary, AB

            // UK postal codes
            Arguments.of(PostalCode.of("SW1A 1AA")), // Westminster, London
            Arguments.of(PostalCode.of("M1 1AA")),   // Manchester
            Arguments.of(PostalCode.of("B33 8TH")),  // Birmingham
            Arguments.of(PostalCode.of("W1A 0AX")),  // London West End
            Arguments.of(PostalCode.of("EC1A 1BB")), // London City

            // German postal codes
            Arguments.of(PostalCode.of("10115")), // Berlin
            Arguments.of(PostalCode.of("80331")), // Munich
            Arguments.of(PostalCode.of("20095")), // Hamburg
            Arguments.of(PostalCode.of("50667")), // Cologne
            Arguments.of(PostalCode.of("60311")), // Frankfurt

            // French postal codes
            Arguments.of(PostalCode.of("75001")), // Paris 1st arrondissement
            Arguments.of(PostalCode.of("13001")), // Marseille
            Arguments.of(PostalCode.of("69001")), // Lyon
            Arguments.of(PostalCode.of("31000")), // Toulouse
            Arguments.of(PostalCode.of("06000")), // Nice

            // Italian postal codes
            Arguments.of(PostalCode.of("00118")), // Rome
            Arguments.of(PostalCode.of("20121")), // Milan
            Arguments.of(PostalCode.of("80121")), // Naples
            Arguments.of(PostalCode.of("10121")), // Turin
            Arguments.of(PostalCode.of("50121")), // Florence

            // Spanish postal codes
            Arguments.of(PostalCode.of("28001")), // Madrid
            Arguments.of(PostalCode.of("08001")), // Barcelona
            Arguments.of(PostalCode.of("41001")), // Seville
            Arguments.of(PostalCode.of("46001")), // Valencia
            Arguments.of(PostalCode.of("48001")), // Bilbao

            // Netherlands postal codes
            Arguments.of(PostalCode.of("1011 PN")), // Amsterdam
            Arguments.of(PostalCode.of("3011 AD")), // Rotterdam
            Arguments.of(PostalCode.of("2511 CV")), // The Hague
            Arguments.of(PostalCode.of("3512 JE")), // Utrecht
            Arguments.of(PostalCode.of("5611 AB")), // Eindhoven

            // Australia postal codes
            Arguments.of(PostalCode.of("2000")), // Sydney, NSW
            Arguments.of(PostalCode.of("3000")), // Melbourne, VIC
            Arguments.of(PostalCode.of("4000")), // Brisbane, QLD
            Arguments.of(PostalCode.of("6000")), // Perth, WA
            Arguments.of(PostalCode.of("5000")), // Adelaide, SA

            // Japan postal codes
            Arguments.of(PostalCode.of("100-0001")), // Tokyo
            Arguments.of(PostalCode.of("530-0001")), // Osaka
            Arguments.of(PostalCode.of("231-0001")), // Yokohama
            Arguments.of(PostalCode.of("460-0001")), // Nagoya
            Arguments.of(PostalCode.of("060-0001")), // Sapporo

            // Brazil postal codes (CEP)
            Arguments.of(PostalCode.of("01310-100")), // São Paulo
            Arguments.of(PostalCode.of("20040-020")), // Rio de Janeiro
            Arguments.of(PostalCode.of("70040-010")), // Brasília
            Arguments.of(PostalCode.of("40070-110")), // Salvador
            Arguments.of(PostalCode.of("30112-000")), // Belo Horizonte

            // India postal codes (PIN)
            Arguments.of(PostalCode.of("110001")), // New Delhi
            Arguments.of(PostalCode.of("400001")), // Mumbai
            Arguments.of(PostalCode.of("700001")), // Kolkata
            Arguments.of(PostalCode.of("600001")), // Chennai
            Arguments.of(PostalCode.of("560001")), // Bangalore

            // China postal codes
            Arguments.of(PostalCode.of("100000")), // Beijing
            Arguments.of(PostalCode.of("200000")), // Shanghai
            Arguments.of(PostalCode.of("510000")), // Guangzhou
            Arguments.of(PostalCode.of("518000")), // Shenzhen
            Arguments.of(PostalCode.of("610000")), // Chengdu

            // South Korea postal codes
            Arguments.of(PostalCode.of("03141")), // Seoul
            Arguments.of(PostalCode.of("48058")), // Busan
            Arguments.of(PostalCode.of("41117")), // Incheon
            Arguments.of(PostalCode.of("42601")), // Daegu
            Arguments.of(PostalCode.of("35242")), // Daejeon

            // Edge cases and special formats
            Arguments.of(PostalCode.of("00501")), // Holtsville, NY (lowest US ZIP)
            Arguments.of(PostalCode.of("99950")), // Ketchikan, AK (highest US ZIP)
            Arguments.of(PostalCode.of("12345")), // Generic test ZIP
            Arguments.of(PostalCode.of("12345-6789")), // Full ZIP+4

            // Short postal codes
            Arguments.of(PostalCode.of("123")),   // 3-digit code
            Arguments.of(PostalCode.of("1234"))  // 4-digit code

        );
    }
}

//////////////////////////////////////////////////////////////////////////////
