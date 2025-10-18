//////////////////////////////////////////////////////////////////////////////
// SerializationConfigurer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import strata.foundation.core.value.*;
import strata.stream.basic.bounded.BasicBoundedStream;
import strata.stream.core.bounded.IBoundedStream;
import strata.stream.core.shared.SuppliedExecutor;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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
                GeoLocation.class,GeoLocationSerializer.class,
                ArrayList.class,CollectionSerializer.class,
                LinkedList.class,CollectionSerializer.class));
    }

    public StreamExecutionEnvironment
    configure(StreamExecutionEnvironment environment)
    {
        Configuration configuration = new Configuration();

        configuration
            .set(
                PipelineOptions.SERIALIZATION_CONFIG,
                getSerializationConfiguration());

        environment.configure(configuration);
        return environment;
    }

    private <T> List<String>
    getSerializationConfiguration()
    {
        List<String> configuration =
            registry
                .entrySet()
                .stream()
                .map(e -> getConfigurationString(e.getKey(),e.getValue()))
                .collect(Collectors.toCollection(ArrayList::new));

        configuration.add(getConfigurationString(IBoundedStream.class));
        configuration.add(getConfigurationString(BasicBoundedStream.class));
        configuration.add(getConfigurationString(Instant.class));
        configuration.add(getConfigurationString(Duration.class));
        configuration.add(getConfigurationString(SuppliedExecutor.class));
        configuration.add(getConfigurationStringOptional());
        configuration.add(getConfigurationStringCompletableFuture());
        return configuration;
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


    private String
    getConfigurationString(Class<?> type)
    {
        return
            String.format(
                "%s: {type: kryo, kryo-type: registered, class: %s}",
                type.getName(),
                JavaSerializer.class.getName());
    }

    private String
    getConfigurationStringOptional()
    {
        return
            String.format(
                "%s: {type: kryo, kryo-type: registered, class: %s}",
                Optional.class.getName(),
                OptionalSerializer.class.getName());
    }


    private String
    getConfigurationStringCompletableFuture()
    {
        return
            String.format(
                "%s: {type: kryo, kryo-type: registered, class: %s}",
                CompletableFuture.class.getName(),
                CompletableFutureSerializer.class.getName());
    }

}

//////////////////////////////////////////////////////////////////////////////
