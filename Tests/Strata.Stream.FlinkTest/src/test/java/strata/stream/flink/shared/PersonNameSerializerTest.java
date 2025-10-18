//////////////////////////////////////////////////////////////////////////////
// PersonNameSerializerTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import strata.foundation.core.value.PersonName;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("CommitStage")
public
class PersonNameSerializerTest
{
    @ParameterizedTest
    @MethodSource("providePersonNames")
    public void
    testSerializeDeserialize(PersonName expected)
    {
        Kryo                 kryo = new Kryo();
        Output               output = new Output(1024,1024*1024);
        Input                input = new Input(output.getBuffer());
        PersonNameSerializer serializer = new PersonNameSerializer();
        PersonName           actual = null;

        kryo.register(PersonName.class,serializer);
        serializer.write(kryo,output,expected);
        output.flush();
        actual = serializer.read(kryo,input,PersonName.class);

        assertEquals(expected,actual);
    }

    public static Stream<Arguments>
    providePersonNames()
    {
        return Stream.of(
            // Basic first and last name only
            Arguments.of(PersonName.of("John", "Doe")),
            Arguments.of(PersonName.of("Jane", "Smith")),
            Arguments.of(PersonName.of("Alice", "Johnson")),
            Arguments.of(PersonName.of("Bob", "Brown")),

            // With middle names
            Arguments.of(PersonName.of("John", "Michael", "Doe")),
            Arguments.of(PersonName.of("Jane", "Elizabeth", "Smith")),
            Arguments.of(PersonName.of("Alice", "Marie", "Johnson")),
            Arguments.of(PersonName.of("Bob", "James", "Brown")),

            // With titles only
            Arguments.of(PersonName.of("Dr.", "John", null, "Doe", null)),
            Arguments.of(PersonName.of("Mr.", "Bob", null, "Brown", null)),
            Arguments.of(PersonName.of("Mrs.", "Jane", null, "Smith", null)),
            Arguments.of(PersonName.of("Ms.", "Alice", null, "Johnson", null)),
            Arguments.of(PersonName.of("Prof.", "David", null, "Wilson", null)),

            // With suffixes only
            Arguments.of(PersonName.of(null, "John", null, "Doe", "Jr.")),
            Arguments.of(PersonName.of(null, "Bob", null, "Brown", "Sr.")),
            Arguments.of(PersonName.of(null, "James", null, "Wilson", "III")),
            Arguments.of(PersonName.of(null, "Robert", null, "Davis", "PhD")),
            Arguments.of(PersonName.of(null, "William", null, "Miller", "MD")),

            // With title and middle name
            Arguments.of(PersonName.of("Dr.", "John", "Michael", "Doe", null)),
            Arguments.of(PersonName.of("Mr.", "Bob", "James", "Brown", null)),
            Arguments.of(PersonName.of("Mrs.", "Jane", "Elizabeth", "Smith", null)),

            // With middle name and suffix
            Arguments.of(PersonName.of(null, "John", "Michael", "Doe", "Jr.")),
            Arguments.of(PersonName.of(null, "Bob", "James", "Brown", "Sr.")),
            Arguments.of(PersonName.of(null, "Jane", "Elizabeth", "Smith", "PhD")),

            // With title and suffix
            Arguments.of(PersonName.of("Dr.", "John", null, "Doe", "Jr.")),
            Arguments.of(PersonName.of("Mr.", "Bob", null, "Brown", "Sr.")),
            Arguments.of(PersonName.of("Prof.", "Alice", null, "Johnson", "PhD")),

            // Complete names with all components
            Arguments.of(PersonName.of("Dr.", "John", "Michael", "Doe", "Jr.")),
            Arguments.of(PersonName.of("Mr.", "Bob", "James", "Brown", "Sr.")),
            Arguments.of(PersonName.of("Mrs.", "Jane", "Elizabeth", "Smith", "PhD")),
            Arguments.of(PersonName.of("Prof.", "Alice", "Marie", "Johnson", "MD")),
            Arguments.of(PersonName.of("Ms.", "Sarah", "Ann", "Davis", "III")),

            // Edge cases with unusual names
            Arguments.of(PersonName.of("Jean-Luc", "Picard")),
            Arguments.of(PersonName.of("Mary", "O'Connor")),
            Arguments.of(PersonName.of("José", "García")),
            Arguments.of(PersonName.of("Mohammed", "Al-Rahman")),

            // Single character names
            Arguments.of(PersonName.of("X", "Y")),
            Arguments.of(PersonName.of("A", "B", "Z")),

            // Long names
            Arguments.of(PersonName.of("Christopher", "Alexander", "Montgomery-Weatherby")),
            Arguments.of(PersonName.of("Sir", "Bartholomew", "Reginald", "Pemberton-Smythe", "Esquire"))
        );
    }
}

//////////////////////////////////////////////////////////////////////////////
