/// ///////////////////////////////////////////////////////////////////////////
// OptionalSerializer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.Optional;

public
class OptionalSerializer
    extends Serializer<Optional<?>>
{
    @Override
    public void
    write(Kryo kryo,Output output,Optional<?> value)
    {
        if (value.isPresent())
        {
            output.writeBoolean(true);
            Object object = value.get();
            kryo.writeClassAndObject(output,object);
        }
        else
            output.writeBoolean(false);
    }

    @Override
    public Optional<?>
    read(Kryo kryo,Input input,Class<Optional<?>> type)
    {
        boolean isPresent = input.readBoolean();

        return
            isPresent
                ? Optional.ofNullable(kryo.readClassAndObject(input))
                : Optional.empty();
    }
}

//////////////////////////////////////////////////////////////////////////////
