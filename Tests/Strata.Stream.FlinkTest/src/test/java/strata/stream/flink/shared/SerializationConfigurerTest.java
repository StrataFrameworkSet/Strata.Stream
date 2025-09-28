/// ///////////////////////////////////////////////////////////////////////////
// SerializationConfigurerTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import strata.foundation.core.value.*;
import strata.stream.basic.bounded.BasicBoundedStream;
import strata.stream.core.bounded.IBoundedStream;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("CommitStage")
public
class SerializationConfigurerTest
{
    @Test
    public void
    testConfigure()
    {
        SerializationConfigurer configurer = new SerializationConfigurer();
        StreamExecutionEnvironment environment =
            configurer.configure(StreamExecutionEnvironment.createLocalEnvironment());
        Map<Class<?>,Class<? extends Serializer<?>>> registeredTypes =
            environment
                .getConfig()
                .getSerializerConfig()
                .getRegisteredTypesWithKryoSerializerClasses();

        assertEquals(
            PersonNameSerializer.class,
            registeredTypes.get(PersonName.class));
        assertEquals(
            PhoneNumberSerializer.class,
            registeredTypes.get(PhoneNumber.class));
        assertEquals(
            EmailAddressSerializer.class,
            registeredTypes.get(EmailAddress.class));
        assertEquals(
            GeoLocationSerializer.class,
            registeredTypes.get(GeoLocation.class));
        assertEquals(
            PostalCodeSerializer.class,
            registeredTypes.get(PostalCode.class));
        assertEquals(
            PostalAddressSerializer.class,
            registeredTypes.get(PostalAddress.class));
        assertEquals(
            JavaSerializer.class,
            registeredTypes.get(IBoundedStream.class));
        assertEquals(
            JavaSerializer.class,
            registeredTypes.get(BasicBoundedStream.class));
        assertEquals(
            JavaSerializer.class,
            registeredTypes.get(Instant.class));
        assertEquals(
            JavaSerializer.class,
            registeredTypes.get(Duration.class));
        assertEquals(
            OptionalSerializer.class,
            registeredTypes.get(Optional.class));

        /*
        assertTrue(
            environment
                .getConfig()
                .getSerializerConfig()
                .isForceKryoEnabled());

         */
    }
}

//////////////////////////////////////////////////////////////////////////////
