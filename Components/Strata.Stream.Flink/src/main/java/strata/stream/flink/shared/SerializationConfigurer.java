//////////////////////////////////////////////////////////////////////////////
// SerializationConfigurer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Serializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import strata.foundation.core.value.*;

import java.util.List;

public
class SerializationConfigurer
{
    public StreamExecutionEnvironment
    configure(StreamExecutionEnvironment environment)
    {
        Configuration configuration = new Configuration();

        configuration
            .set(
                PipelineOptions.SERIALIZATION_CONFIG,
                List.of(
                    getConfigurationString(PersonName.class,PersonNameSerializer.class),
                    getConfigurationString(PhoneNumber.class,PhoneNumberSerializer.class),
                    getConfigurationString(EmailAddress.class,EmailAddressSerializer.class),
                    getConfigurationString(PostalAddress.class,PostalAddressSerializer.class),
                    getConfigurationString(PostalCode.class,PostalCodeSerializer.class),
                    getConfigurationString(GeoLocation.class,GeoLocationSerializer.class)));

        environment.configure(configuration);
        return environment;
    }

    private <T> String
    getConfigurationString(Class<T> type,Class<? extends Serializer<T>> serializerType)
    {
        return
            String.format(
                "%s: {type: kryo, kryo-type: registered, class: %s}",
                type.getName(),
                serializerType.getName());
    }
}

//////////////////////////////////////////////////////////////////////////////
