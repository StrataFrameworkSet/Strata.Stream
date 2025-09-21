/// ///////////////////////////////////////////////////////////////////////////
// PostalAddressSerializer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import strata.foundation.core.value.PostalAddress;

import java.util.Objects;

public
class PostalAddressSerializer
    extends Serializer<PostalAddress>
{
    @Override
    public void
    write(Kryo kryo,Output output,PostalAddress postalAddress)
    {
        output.writeString(toString(postalAddress));
    }

    @Override
    public PostalAddress
    read(Kryo kryo,Input input,Class<? extends PostalAddress> type)
    {
        return fromString(input.readString());
    }

    private String
    toString(PostalAddress postalAddress)
    {
        Objects.requireNonNull(postalAddress);

        return
            String.format(
                "%s|%s|%s|%s|%s|%s",
                getOutput(postalAddress.getAddress()),
                getOutput(postalAddress.getStreet()),
                getOutput(postalAddress.getCity()),
                getOutput(postalAddress.getState()),
                getOutput(postalAddress.getCountryCode()),
                getOutput(postalAddress.getPostalCode()));
    }

    private PostalAddress
    fromString(String data)
    {
        Objects.requireNonNull(data);

        String[] parts = data.split("\\|");

        if (parts.length != 6)
            throw
                new IllegalArgumentException("Invalid postal address: " + data);

        return
            new PostalAddress(
                getInput(parts[0]),
                getInput(parts[1]),
                getInput(parts[2]),
                getInput(parts[3]),
                getInput(parts[4]),
                getInput(parts[5]));
    }

    private String
    getOutput(String value)
    {
        return value == null || value == "" ? "*" : value;
    }

    private String
    getInput(String value)
    {
        return value.equals("*") ? "" : value;
    }
}

//////////////////////////////////////////////////////////////////////////////
