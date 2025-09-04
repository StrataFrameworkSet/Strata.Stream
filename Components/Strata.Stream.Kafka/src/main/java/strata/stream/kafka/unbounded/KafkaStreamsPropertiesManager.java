//////////////////////////////////////////////////////////////////////////////
// KafkaStreamsPropertiesManager.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public
class KafkaStreamsPropertiesManager
{
    private static String   applicationId;
    private static String   bootstrapServers;
    private static Class<?> defaultKeySerdeClass;
    private static Class<?> defaultValueSerdeClass;

    public static void
    setApplicationId(String id)
    {
        applicationId = id;
    }

    public static void
    setBootstrapServers(String servers)
    {
        bootstrapServers = servers;
    }

    public static void
    setDefaultKeySerdeClass(Class<?> keySerdeClass)
    {
        defaultKeySerdeClass = keySerdeClass;
    }

    public static void
    setDefaultValueSerdeClass(Class<?> valueSerdeClass)
    {
        defaultValueSerdeClass = valueSerdeClass;
    }

    public static Properties
    getProperties()
    {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, defaultKeySerdeClass);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, defaultValueSerdeClass);

        return props;
    }
}

//////////////////////////////////////////////////////////////////////////////
