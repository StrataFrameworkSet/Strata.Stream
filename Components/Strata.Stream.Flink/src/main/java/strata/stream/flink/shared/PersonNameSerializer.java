//////////////////////////////////////////////////////////////////////////////
// PersonNameSerializer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import strata.foundation.core.value.PersonName;

public
class PersonNameSerializer
    extends Serializer<PersonName>
{
    @Override
    public void
    write(Kryo kryo,Output output,PersonName personName)
    {
        output.writeString(toString(personName));
    }

    @Override
    public PersonName
    read(Kryo kryo,Input input,Class<PersonName> aClass)
    {
        return fromString(input.readString());
    }

    private String
    toString(PersonName personName)
    {
        return
            String.format(
                "%s|%s|%s|%s|%s",
                personName.getTitle().orElse("*"),
                personName.getFirstName(),
                personName.getMiddleName().orElse("*"),
                personName.getLastName(),
                personName.getSuffix().orElse("*"));
    }

    private PersonName
    fromString(String s)
    {
        String[] parts = s.split("\\|");

        if (parts.length != 5)
            throw
                new IllegalArgumentException("Invalid person name string: " + s);

        return
            PersonName.of(
                getOptional(parts[0]),
                parts[1],
                getOptional(parts[2]),
                parts[3],
                getOptional(parts[4]));
    }

    private String
    getOptional(String field)
    {
        return field.equals("*") ? null : field;
    }
}

//////////////////////////////////////////////////////////////////////////////
