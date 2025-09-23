//////////////////////////////////////////////////////////////////////////////
// PhoneNumberSerializer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import strata.foundation.core.value.PhoneNumber;

public
class PhoneNumberSerializer
    extends Serializer<PhoneNumber>
{
    @Override
    public void
    write(Kryo kryo,Output output,PhoneNumber phoneNumber)
    {
        output.writeString(phoneNumber.toString());
    }

    @Override
    public PhoneNumber
    read(Kryo kryo,Input input,Class<PhoneNumber> type)
    {
        return PhoneNumber.of(input.readString());
    }
}

//////////////////////////////////////////////////////////////////////////////
