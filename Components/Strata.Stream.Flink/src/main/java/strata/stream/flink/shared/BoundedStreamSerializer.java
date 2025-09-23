/// ///////////////////////////////////////////////////////////////////////////
// BoundedStreamSerializer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import strata.stream.basic.bounded.BasicBoundedStream;
import strata.stream.core.bounded.IBoundedStream;

import java.io.Serializable;

public
class BoundedStreamSerializer<T extends Serializable>
    extends Serializer<IBoundedStream<T>>
{
    private final JavaSerializer serializer;

    public
    BoundedStreamSerializer()
    {
        serializer = new JavaSerializer();
    }

    @Override
    public void
    write(Kryo kryo,Output output,IBoundedStream<T> value)
    {
        kryo.writeObject(output,value,serializer);
    }

    @Override
    public IBoundedStream<T>
    read(Kryo kryo,Input input,Class<IBoundedStream<T>> type)
    {
        return kryo.readObject(input,BasicBoundedStream.class,serializer);
    }
}

//////////////////////////////////////////////////////////////////////////////
