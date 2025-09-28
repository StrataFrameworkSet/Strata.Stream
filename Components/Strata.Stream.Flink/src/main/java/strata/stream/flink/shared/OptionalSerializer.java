/// ///////////////////////////////////////////////////////////////////////////
// OptionalSerializer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;
import java.util.Optional;

public
class OptionalSerializer<T extends Serializable>
    extends Serializer<Optional<T>>
{
    @Override
    public void
    write(Kryo kryo,Output output,Optional<T> value)
    {
         kryo.writeObjectOrNull(
            output,
            value.orElse(null),
            kryo.getSerializer(Object.class));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Optional<T>
    read(Kryo kryo,Input input,Class<Optional<T>> type)
    {
        return
            Optional.ofNullable(
                (T)kryo.readObjectOrNull(
                    input,
                    Object.class,
                    kryo.getSerializer(Object.class)));
    }
}

//////////////////////////////////////////////////////////////////////////////
