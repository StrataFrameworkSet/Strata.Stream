//////////////////////////////////////////////////////////////////////////////
// EmailAddressSerializerTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import strata.foundation.core.value.EmailAddress;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("CommitStage")
public
class EmailAddressSerializerTest
{
    @ParameterizedTest
    @MethodSource("provideEmailAddresses")
    public void
    testSerializeDeserialize(EmailAddress expected)
    {
        Kryo                    kryo = new Kryo();
        Output                  output = new Output(1024,1024*1024);
        Input                   input = new Input(output.getBuffer());
        EmailAddressSerializer  serializer = new EmailAddressSerializer();
        EmailAddress            actual = null;

        kryo.register(EmailAddress.class,serializer);
        serializer.write(kryo,output,expected);
        output.flush();
        actual = serializer.read(kryo,input,EmailAddress.class);

        assertEquals(expected,actual);
    }

    public static Stream<Arguments>
    provideEmailAddresses()
    {
        return Stream.of(
            // Basic email addresses
            Arguments.of(EmailAddress.of("john@example.com")),
            Arguments.of(EmailAddress.of("jane.smith@company.org")),
            Arguments.of(EmailAddress.of("alice.johnson@university.edu")),
            Arguments.of(EmailAddress.of("bob.brown@government.gov")),

            // Email addresses with numbers
            Arguments.of(EmailAddress.of("user123@domain.com")),
            Arguments.of(EmailAddress.of("test2024@example.org")),
            Arguments.of(EmailAddress.of("admin007@secure.net")),
            Arguments.of(EmailAddress.of("support99@helpdesk.io")),

            // Email addresses with hyphens and underscores
            Arguments.of(EmailAddress.of("first-last@company.com")),
            Arguments.of(EmailAddress.of("user_name@domain.org")),
            Arguments.of(EmailAddress.of("test-email@sub-domain.net")),
            Arguments.of(EmailAddress.of("my_email_address@long-domain-name.info")),

            // Email addresses with plus signs (common for email aliasing)
            Arguments.of(EmailAddress.of("user+tag@gmail.com")),
            Arguments.of(EmailAddress.of("john+work@company.org")),
            Arguments.of(EmailAddress.of("jane+newsletter@domain.net")),
            Arguments.of(EmailAddress.of("test+spam@filter.com")),

            // Short email addresses
            Arguments.of(EmailAddress.of("a@b.co")),
            Arguments.of(EmailAddress.of("x@y.io")),
            Arguments.of(EmailAddress.of("i@me.org")),

            // Long email addresses
            Arguments.of(EmailAddress.of("very.long.email.address.with.many.dots@extremely.long.domain.name.with.subdomains.com")),
            Arguments.of(EmailAddress.of("super-long-username-with-hyphens@super-long-domain-name-with-multiple-subdomains.organization")),
            Arguments.of(EmailAddress.of("department.subdivision.team.member@large.corporation.international.business.com")),

            // International domain extensions
            Arguments.of(EmailAddress.of("user@domain.uk")),
            Arguments.of(EmailAddress.of("contact@company.de")),
            Arguments.of(EmailAddress.of("info@organization.fr")),
            Arguments.of(EmailAddress.of("support@business.jp")),
            Arguments.of(EmailAddress.of("admin@website.au")),

            // Special cases with dots
            Arguments.of(EmailAddress.of("first.middle.last@domain.com")),
            Arguments.of(EmailAddress.of("a.b.c.d@e.f.g.h.com")),
            Arguments.of(EmailAddress.of("multiple.dots.in.username@multiple.dots.in.domain.org")),

            // Common email providers
            Arguments.of(EmailAddress.of("user@gmail.com")),
            Arguments.of(EmailAddress.of("person@yahoo.com")),
            Arguments.of(EmailAddress.of("contact@outlook.com")),
            Arguments.of(EmailAddress.of("admin@hotmail.com")),
            Arguments.of(EmailAddress.of("business@protonmail.com")),

            // Mixed case (though typically normalized to lowercase)
            Arguments.of(EmailAddress.of("User@Domain.Com")),
            Arguments.of(EmailAddress.of("ADMIN@COMPANY.ORG")),
            Arguments.of(EmailAddress.of("MixedCase@Example.Net")),

            // Edge cases with valid special characters
            Arguments.of(EmailAddress.of("x@example.com")),
            Arguments.of(EmailAddress.of("example@s.example"))
        );
    }
}

//////////////////////////////////////////////////////////////////////////////
