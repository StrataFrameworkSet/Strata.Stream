//////////////////////////////////////////////////////////////////////////////
// BasicBoundedStreamSerializer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.kafka.common.utils.Java;
import strata.stream.basic.bounded.BasicBoundedStream;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public
class BasicBoundedStreamSerializer<T extends Serializable>
    extends Serializer<BasicBoundedStream<T>>
{
    private final JavaSerializer serializer;

    public
    BasicBoundedStreamSerializer()
    {
        serializer = new JavaSerializer();
    }

    @Override
    public void
    write(Kryo kryo,Output output,BasicBoundedStream<T> value)
    {
        kryo.writeObject(output,value,serializer);
    }

    @Override
    public BasicBoundedStream<T>
    read(Kryo kryo,Input input,Class<BasicBoundedStream<T>> type)
    {
        return kryo.readObject(input,BasicBoundedStream.class,serializer);
    }
}

//////////////////////////////////////////////////////////////////////////////
