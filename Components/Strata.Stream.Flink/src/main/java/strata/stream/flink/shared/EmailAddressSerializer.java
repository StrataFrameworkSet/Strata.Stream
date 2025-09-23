//////////////////////////////////////////////////////////////////////////////
// EmailAddressSerializer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import strata.foundation.core.value.EmailAddress;

public
class EmailAddressSerializer
    extends Serializer<EmailAddress>
{
    @Override
    public void
    write(Kryo kryo,Output output,EmailAddress emailAddress)
    {
        output.writeString(emailAddress.toString());
    }

    @Override
    public EmailAddress
    read(Kryo kryo,Input input,Class<EmailAddress> type)
    {
        return EmailAddress.of(input.readString());
    }
}

//////////////////////////////////////////////////////////////////////////////
