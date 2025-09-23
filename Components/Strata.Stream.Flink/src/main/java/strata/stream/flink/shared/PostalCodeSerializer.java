//////////////////////////////////////////////////////////////////////////////
// PostalCodeSerializer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import strata.foundation.core.value.PostalCode;

public
class PostalCodeSerializer
    extends Serializer<PostalCode>
{
    @Override
    public void
    write(Kryo kryo,Output output,PostalCode postalCode)
    {
        output.writeString(postalCode.toString());
    }

    @Override
    public PostalCode
    read(Kryo kryo,Input input,Class<PostalCode> type)
    {
        return PostalCode.of(input.readString());
    }
}

//////////////////////////////////////////////////////////////////////////////
