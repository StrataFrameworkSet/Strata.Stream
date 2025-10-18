//////////////////////////////////////////////////////////////////////////////
// SerializableSerializer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public
class SerializableSerializer<T>
    implements Serializer<T>
{
    public
    SerializableSerializer() {}

    @Override
    public byte[]
    serialize(String topic,T data)
    {
        if (data instanceof Serializable serializable)
        {
            try (
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream    oos = new ObjectOutputStream(baos))
            {
                oos.writeObject(serializable);
                oos.flush();
                return baos.toByteArray();
            }
            catch (Exception e)
            {
                throw new RuntimeException("Serialization error", e);
            }
        }

        throw new IllegalArgumentException("Data must implement Serializable");
    }
}

//////////////////////////////////////////////////////////////////////////////
