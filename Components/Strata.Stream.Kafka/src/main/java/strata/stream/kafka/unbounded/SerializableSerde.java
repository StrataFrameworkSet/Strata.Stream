//////////////////////////////////////////////////////////////////////////////
// SerializableSerde.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public
class SerializableSerde<T>
    implements Serde<T>
{
    @Override
    public Serializer<T>
    serializer()
    {
        return new SerializableSerializer<>();
    }

    @Override
    public Deserializer<T>
    deserializer()
    {
        return new SerializableDeserializer<>();
    }
}

//////////////////////////////////////////////////////////////////////////////
