//////////////////////////////////////////////////////////////////////////////
// PhoneNumberSerializerTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import strata.foundation.core.value.PhoneNumber;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("CommitStage")
public
class PhoneNumberSerializerTest
{
    @ParameterizedTest
    @MethodSource("providePhoneNumbers")
    public void
    testSerializeDeserialize(PhoneNumber expected)
    {
        Kryo                    kryo = new Kryo();
        Output                  output = new Output(1024,1024*1024);
        Input                   input = new Input(output.getBuffer());
        PhoneNumberSerializer   serializer = new PhoneNumberSerializer();
        PhoneNumber             actual = null;

        kryo.register(PhoneNumber.class,serializer);
        serializer.write(kryo,output,expected);
        output.flush();
        actual = serializer.read(kryo,input,PhoneNumber.class);

        assertEquals(expected,actual);
    }

    public static Stream<Arguments>
    providePhoneNumbers()
    {
        return Stream.of(
            // NANP Pattern: ^([1][-.\\s]?)?\\(?([0-9]{3})\\)?[-.\\s]?([0-9]{3})[-.\\s]?([0-9]{4})$

            // Basic NANP format without country code
            Arguments.of(PhoneNumber.of("(212)555-1234")),
            Arguments.of(PhoneNumber.of("(415)123-4567")),
            Arguments.of(PhoneNumber.of("(310)987-6543")),
            Arguments.of(PhoneNumber.of("(713)456-7890")),

            // NANP with dashes and no parentheses
            Arguments.of(PhoneNumber.of("212-555-1234")),
            Arguments.of(PhoneNumber.of("415-123-4567")),
            Arguments.of(PhoneNumber.of("310-987-6543")),
            Arguments.of(PhoneNumber.of("713-456-7890")),

            // NANP with dots
            Arguments.of(PhoneNumber.of("212.555.1234")),
            Arguments.of(PhoneNumber.of("415.123.4567")),
            Arguments.of(PhoneNumber.of("310.987.6543")),
            Arguments.of(PhoneNumber.of("713.456.7890")),

            // NANP with spaces
            Arguments.of(PhoneNumber.of("212 555 1234")),
            Arguments.of(PhoneNumber.of("415 123 4567")),
            Arguments.of(PhoneNumber.of("310 987 6543")),
            Arguments.of(PhoneNumber.of("713 456 7890")),

            // NANP with country code 1 and dashes
            Arguments.of(PhoneNumber.of("1-212-555-1234")),
            Arguments.of(PhoneNumber.of("1-415-123-4567")),
            Arguments.of(PhoneNumber.of("1-310-987-6543")),
            Arguments.of(PhoneNumber.of("1-713-456-7890")),

            // NANP with country code 1 and dots
            Arguments.of(PhoneNumber.of("1.212.555.1234")),
            Arguments.of(PhoneNumber.of("1.415.123.4567")),
            Arguments.of(PhoneNumber.of("1.310.987.6543")),
            Arguments.of(PhoneNumber.of("1.713.456.7890")),

            // NANP with country code 1 and spaces
            Arguments.of(PhoneNumber.of("1 212 555 1234")),
            Arguments.of(PhoneNumber.of("1 415 123 4567")),
            Arguments.of(PhoneNumber.of("1 310 987 6543")),
            Arguments.of(PhoneNumber.of("1 713 456 7890")),

            // NANP with country code 1 and parentheses
            Arguments.of(PhoneNumber.of("1-(212)555-1234")),
            Arguments.of(PhoneNumber.of("1-(415)123-4567")),
            Arguments.of(PhoneNumber.of("1-(310)987-6543")),
            Arguments.of(PhoneNumber.of("1-(713)456-7890")),

            // NANP toll-free numbers
            Arguments.of(PhoneNumber.of("(800)555-1234")),
            Arguments.of(PhoneNumber.of("(888)123-4567")),
            Arguments.of(PhoneNumber.of("(877)987-6543")),
            Arguments.of(PhoneNumber.of("(866)456-7890")),

            // ITU-T Pattern: ^\\+(?:[0-9] ?){6,14}[0-9]$

            // ITU-T format with various country codes
            Arguments.of(PhoneNumber.of("+1 2125551234")),
            Arguments.of(PhoneNumber.of("+44 2079460958")),
            Arguments.of(PhoneNumber.of("+49 3012345678")),
            Arguments.of(PhoneNumber.of("+33 142868326")),
            Arguments.of(PhoneNumber.of("+81 312345678")),
            Arguments.of(PhoneNumber.of("+86 1012345678")),

            // ITU-T format without spaces
            Arguments.of(PhoneNumber.of("+12125551234")),
            Arguments.of(PhoneNumber.of("+442079460958")),
            Arguments.of(PhoneNumber.of("+4930123456")),
            Arguments.of(PhoneNumber.of("+33142868326")),
            Arguments.of(PhoneNumber.of("+81312345678")),
            Arguments.of(PhoneNumber.of("+861012345678")),

            // ITU-T format with spaced digits
            Arguments.of(PhoneNumber.of("+1 2 1 2 5 5 5 1 2 3 4")),
            Arguments.of(PhoneNumber.of("+4 4 2 0 7 9 4 6 0 9 5 8")),
            Arguments.of(PhoneNumber.of("+4 9 3 0 1 2 3 4 5 6 7 8")),
            Arguments.of(PhoneNumber.of("+3 3 1 4 2 8 6 8 3 2 6")),

            // ITU-T mobile numbers
            Arguments.of(PhoneNumber.of("+447911123456")),
            Arguments.of(PhoneNumber.of("+4915112345678")),
            Arguments.of(PhoneNumber.of("+33612345678")),
            Arguments.of(PhoneNumber.of("+819012345678")),
            Arguments.of(PhoneNumber.of("+8613812345678")),

            // ITU-T shorter numbers (6-7 digits after country code)
            Arguments.of(PhoneNumber.of("+41123456")),
            Arguments.of(PhoneNumber.of("+3581234567")),
            Arguments.of(PhoneNumber.of("+4612345678")),

            // EPP Pattern: ^\\+[0-9]{1,3}\\.[0-9]{4,14}(?:x.+)?$

            // EPP format basic
            Arguments.of(PhoneNumber.of("+1.2125551234")),
            Arguments.of(PhoneNumber.of("+44.2079460958")),
            Arguments.of(PhoneNumber.of("+49.3012345678")),
            Arguments.of(PhoneNumber.of("+33.142868326")),
            Arguments.of(PhoneNumber.of("+81.312345678")),
            Arguments.of(PhoneNumber.of("+86.1012345678")),

            // EPP format with 2-digit country codes
            Arguments.of(PhoneNumber.of("+44.20794609581")),
            Arguments.of(PhoneNumber.of("+49.301234567890")),
            Arguments.of(PhoneNumber.of("+33.1428683261234")),
            Arguments.of(PhoneNumber.of("+81.31234567890")),

            // EPP format with 3-digit country codes
            Arguments.of(PhoneNumber.of("+358.501234567")),
            Arguments.of(PhoneNumber.of("+372.51234567")),
            Arguments.of(PhoneNumber.of("+371.21234567")),
            Arguments.of(PhoneNumber.of("+370.61234567")),

            // EPP format with extensions
            Arguments.of(PhoneNumber.of("+1.2125551234x123")),
            Arguments.of(PhoneNumber.of("+44.2079460958x456")),
            Arguments.of(PhoneNumber.of("+49.3012345678x789")),
            Arguments.of(PhoneNumber.of("+33.142868326x101")),
            Arguments.of(PhoneNumber.of("+81.312345678x202")),

            // EPP format with longer extensions
            Arguments.of(PhoneNumber.of("+1.2125551234x12345")),
            Arguments.of(PhoneNumber.of("+44.2079460958xABC123")),
            Arguments.of(PhoneNumber.of("+49.3012345678xEXT456")),
            Arguments.of(PhoneNumber.of("+33.142868326x#789")),

            // EPP format minimum length (4 digits after country code)
            Arguments.of(PhoneNumber.of("+1.1234")),
            Arguments.of(PhoneNumber.of("+44.5678")),
            Arguments.of(PhoneNumber.of("+49.9012")),
            Arguments.of(PhoneNumber.of("+358.3456")),

            // EPP format maximum length (14 digits after country code)
            Arguments.of(PhoneNumber.of("+1.12345678901234")),
            Arguments.of(PhoneNumber.of("+44.56789012345678")),
            Arguments.of(PhoneNumber.of("+358.90123456789012"))
        );
    }
}

//////////////////////////////////////////////////////////////////////////////
