//////////////////////////////////////////////////////////////////////////////
// ReflectiveSerde.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import strata.foundation.core.reflect.TypeLiteral;

public
class ReflectiveSerde<T>
    implements Serde<T>
{
    private final Serde<T> serde;
    private Logger logger;

    public
    ReflectiveSerde()
    {
        logger = LogManager.getLogger(getClass());
        logger.debug("ReflectiveSerde()");
        serde = new SerializableSerde<>();
    }

    public
    ReflectiveSerde(Class<T> type)
    {
        logger = LogManager.getLogger(getClass());
        logger.debug("ReflectiveSerde({})", type);
        serde =
            SerdesMapper
                .getInstance()
                .map(type)
                .orElse(Serdes.serdeFrom(type));
    }

    public
    ReflectiveSerde(TypeLiteral<T> type)
    {
        logger = LogManager.getLogger(getClass());
        logger.debug("ReflectiveSerde({})", type);
        serde =
            SerdesMapper
                .getInstance()
                .map(type)
                .orElse(Serdes.serdeFrom(type.getRawType()));
    }

    @Override
    public Serializer<T>
    serializer()
    {
        return serde.serializer();
    }

    @Override
    public Deserializer<T>
    deserializer()
    {
        return serde.deserializer();
    }

    public static <T> ReflectiveSerde<T>
    of(Class<T> type)
    {
        return new ReflectiveSerde<>(type);
    }

    public static <T> ReflectiveSerde<T>
    of(TypeLiteral<T> type)
    {
        return new ReflectiveSerde<>(type);
    }
}

//////////////////////////////////////////////////////////////////////////////
