/// ///////////////////////////////////////////////////////////////////////////
// PostalAddressSerializerTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import strata.foundation.core.value.PostalAddress;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("CommitStage")
public
class PostalAddressSerializerTest
{
    @ParameterizedTest
    @MethodSource("providePostalAddresses")
    public void
    testSerializeDeserialize(PostalAddress expected)
    {
        Kryo                        kryo = new Kryo();
        Output                      output = new Output(1024,1024*1024);
        Input                       input = new Input(output.getBuffer());
        PostalAddressSerializer     serializer = new PostalAddressSerializer();
        PostalAddress               actual = null;

        kryo.register(PostalAddress.class,serializer);
        serializer.write(kryo,output,expected);
        output.flush();
        actual = serializer.read(kryo,input,PostalAddress.class);

        assertEquals(expected,actual);
    }

    public static Stream<Arguments>
    providePostalAddresses()
    {
        return Stream.of(
            // Complete US addresses
            Arguments.of(new PostalAddress("350", "Fifth Avenue", "New York", "NY", "10118", "US")),
            Arguments.of(new PostalAddress("1600", "Pennsylvania Avenue NW", "Washington", "DC", "20500", "US")),
            Arguments.of(new PostalAddress("1", "Infinite Loop", "Cupertino", "CA", "95014", "US")),
            Arguments.of(new PostalAddress("1", "Microsoft Way", "Redmond", "WA", "98052", "US")),
            Arguments.of(new PostalAddress("233", "S Wacker Dr", "Chicago", "IL", "60606", "US")),

            // Canadian addresses
            Arguments.of(new PostalAddress("111", "Wellington St", "Ottawa", "ON", "K1A 0A6", "CA")),
            Arguments.of(new PostalAddress("290", "Bremner Blvd", "Toronto", "ON", "M5V 3L9", "CA")),
            Arguments.of(new PostalAddress("1055", "Dunsmuir St", "Vancouver", "BC", "V7X 1L3", "CA")),
            Arguments.of(new PostalAddress("615", "Macleod Trail SE", "Calgary", "AB", "T2G 4T8", "CA")),
            Arguments.of(new PostalAddress("360", "Albert St", "Winnipeg", "MB", "R3B 2V6", "CA")),

            // UK addresses
            Arguments.of(new PostalAddress("10", "Downing Street", "London", "", "SW1A 2AA", "GB")),
            Arguments.of(new PostalAddress("221B", "Baker Street", "London", "", "NW1 6XE", "GB")),
            Arguments.of(new PostalAddress("1", "Deansgate", "Manchester", "", "M1 5DB", "GB")),
            Arguments.of(new PostalAddress("", "Royal Mile", "Edinburgh", "", "EH1 2PB", "GB")),
            Arguments.of(new PostalAddress("", "Broad Street", "Birmingham", "", "B1 2EA", "GB")),

            // German addresses
            Arguments.of(new PostalAddress("77", "Unter den Linden", "Berlin", "", "10117", "DE")),
            Arguments.of(new PostalAddress("1", "Marienplatz", "München", "Bayern", "80331", "DE")),
            Arguments.of(new PostalAddress("7", "Mönckebergstraße", "Hamburg", "", "20095", "DE")),
            Arguments.of(new PostalAddress("92", "Königsallee", "Düsseldorf", "NRW", "40212", "DE")),
            Arguments.of(new PostalAddress("106", "Zeil", "Frankfurt am Main", "Hessen", "60313", "DE")),

            // French addresses
            Arguments.of(new PostalAddress("", "Avenue des Champs-Élysées", "Paris", "", "75008", "FR")),
            Arguments.of(new PostalAddress("", "La Canebière", "Marseille", "", "13001", "FR")),
            Arguments.of(new PostalAddress("", "Place Bellecour", "Lyon", "", "69002", "FR")),
            Arguments.of(new PostalAddress("", "Place du Capitole", "Toulouse", "", "31000", "FR")),
            Arguments.of(new PostalAddress("", "Promenade des Anglais", "Nice", "", "06000", "FR")),

            // Japanese addresses
            Arguments.of(new PostalAddress("1-1", "Shibuya", "Tokyo", "", "150-0002", "JP")),
            Arguments.of(new PostalAddress("1-1", "Umeda", "Osaka", "", "530-0001", "JP")),
            Arguments.of(new PostalAddress("2-2-1", "Minato Mirai", "Yokohama", "", "220-0012", "JP")),
            Arguments.of(new PostalAddress("3-1-1", "Marunouchi", "Nagoya", "", "460-0002", "JP")),
            Arguments.of(new PostalAddress("1-1", "Susukino", "Sapporo", "", "064-0804", "JP")),

            // Australian addresses
            Arguments.of(new PostalAddress("1", "Macquarie Street", "Sydney", "NSW", "2000", "AU")),
            Arguments.of(new PostalAddress("", "Collins Street", "Melbourne", "VIC", "3000", "AU")),
            Arguments.of(new PostalAddress("", "Queen Street", "Brisbane", "QLD", "4000", "AU")),
            Arguments.of(new PostalAddress("", "St Georges Terrace", "Perth", "WA", "6000", "AU")),
            Arguments.of(new PostalAddress("", "King William Street", "Adelaide", "SA", "5000", "AU")),

            // Addresses with missing components (empty strings)
            Arguments.of(new PostalAddress("123", "Main St", "Anytown", "ST", "12345", "US")),
            Arguments.of(new PostalAddress("", "Main Street", "Springfield", "IL", "62701", "US")),
            Arguments.of(new PostalAddress("456", "Oak Avenue", "", "CA", "90210", "US")),
            Arguments.of(new PostalAddress("789", "Pine Road", "Somewhere", "", "54321", "US")),
            Arguments.of(new PostalAddress("321", "Elm Street", "Nowhere", "TX", "", "US")),

            // Addresses with null components
            Arguments.of(new PostalAddress(null, "Broadway", "New York", "NY", "10001", "US")),
            Arguments.of(new PostalAddress("", "Wall Street", "New York", "NY", "10005", "US")),
            Arguments.of(new PostalAddress("", "Fifth Avenue", null, "NY", "10022", "US")),
            Arguments.of(new PostalAddress("", "Park Avenue", "New York", null, "10016", "US")),
            Arguments.of(new PostalAddress("", "Madison Avenue", "New York", "NY", null, "US")),

            // International addresses with special characters
            Arguments.of(new PostalAddress("123", "Champs-Élysées", "Paris", "Île-de-France", "75008", "FR")),
            Arguments.of(new PostalAddress("456", "Rua São João", "São Paulo", "SP", "01035-000", "BR")),
            Arguments.of(new PostalAddress("789", "Αθηνάς", "Αθήνα", "", "10554", "GR")),
            Arguments.of(new PostalAddress("12", "Пушкинская", "Москва", "", "109012", "RU")),
            Arguments.of(new PostalAddress("1-1-1", "西新宿", "新宿区", "", "160-0023", "JP")),

            // Long addresses
            Arguments.of(new PostalAddress("Suite 1234", "Very Long Street Name That Goes On And On", "Very Long City Name With Multiple Words", "VeryLongStateName", "12345-6789", "US")),
            Arguments.of(new PostalAddress("Building A", "Extraordinarily Long Street Name Boulevard", "Exceptionally Long City Name", "XtraLongState", "98765-4321", "US")),

            // Short addresses
            Arguments.of(new PostalAddress("1", "A St", "B", "C", "1", "D")),
            Arguments.of(new PostalAddress("2", "X", "Y", "Z", "22", "AA"))
        );
    }
}

//////////////////////////////////////////////////////////////////////////////
