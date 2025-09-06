/// ///////////////////////////////////////////////////////////////////////////
// SerializableDeserializer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

public
class SerializableDeserializer<T>
    implements Deserializer<T>
{
    public
    SerializableDeserializer() {}

    @Override
    public T
    deserialize(String topic,byte[] data)
    {
        if (data != null)
        {
            try (
                ByteArrayInputStream bais = new ByteArrayInputStream(data);
                ObjectInputStream    ois = new ObjectInputStream(bais))
            {
                @SuppressWarnings("unchecked")
                T output = (T)ois.readObject();

                return output;
            }
            catch (Exception e)
            {
                throw new RuntimeException("Deserialization failed", e);
            }
        }

        return null;
    }
}

//////////////////////////////////////////////////////////////////////////////
