//////////////////////////////////////////////////////////////////////////////
// SerializationConfigurer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Serializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import strata.foundation.core.value.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public
class SerializationConfigurer
{
    private final Map<Class<?>,Class<? extends Serializer<?>>> registry;

    public
    SerializationConfigurer(Map<Class<?>,Class<? extends Serializer<?>>> registry)
    {
        this();
        this.registry.putAll(registry);
    }

    public
    SerializationConfigurer()
    {
        registry = new LinkedHashMap<>();
        registry.putAll(
            Map.of(
                PersonName.class,PersonNameSerializer.class,
                PhoneNumber.class,PhoneNumberSerializer.class,
                EmailAddress.class,EmailAddressSerializer.class,
                PostalAddress.class,PostalAddressSerializer.class,
                PostalCode.class,PostalCodeSerializer.class,
                GeoLocation.class,GeoLocationSerializer.class));
    }

    public StreamExecutionEnvironment
    configure(StreamExecutionEnvironment environment)
    {
        Configuration configuration = new Configuration();

        configuration
            .set(
                PipelineOptions.SERIALIZATION_CONFIG,
                getSerializationConfiguration())
            .set(PipelineOptions.FORCE_KRYO,true);

        environment.configure(configuration);
        return environment;
    }

    private <T> List<String>
    getSerializationConfiguration()
    {
        return
            registry
                .entrySet()
                .stream()
                .map(e -> getConfigurationString(e.getKey(),e.getValue()))
                .toList();
    }

    private String
    getConfigurationString(Class<?> type,Class<? extends Serializer<?>> serializerType)
    {
        return
            String.format(
                "%s: {type: kryo, kryo-type: registered, class: %s}",
                type.getName(),
                serializerType.getName());
    }
}

//////////////////////////////////////////////////////////////////////////////
