//////////////////////////////////////////////////////////////////////////////
// KafkaUnboundedStreamTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import strata.stream.basic.unbounded.MapUnboundedStreamSink;
import strata.stream.core.unbounded.AbstractUnboundedStreamTest;
import strata.stream.core.unbounded.IExecutionDriver;
import strata.stream.core.unbounded.IUnboundedStream;
import strata.stream.core.unbounded.ProcessMethodAccessor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;

@Tag("CommitStage")
public
class KafkaUnboundedStreamTest
    extends AbstractUnboundedStreamTest
{
    private IExecutionDriver driver;

    @BeforeAll
    public static void
    setUpAll()
    {
        KafkaStreamsPropertiesManager.setApplicationId("kafka-unbounded-streams-test");
        KafkaStreamsPropertiesManager.setBootstrapServers("localhost:8080");
        KafkaStreamsPropertiesManager.setDefaultKeySerdeClass(Serdes.Long().getClass());
        KafkaStreamsPropertiesManager.setDefaultValueSerdeClass(Serdes.String().getClass());
    }

    @Override
    @Test
    public void
    testKeyBy() throws Exception
    {
        super.testKeyBy();
    }

    @Override
    //@Test
    public void
    testWindowBy() throws Exception
    {
        super.testWindowBy();
    }

    @Override
    @Test
    public void
    testFilter() throws Exception
    {
        super.testFilter();
    }

    @Override
    @Test
    public void
    testMap() throws Exception
    {
        super.testMap();
    }

    @Override
    @Test
    public void
    testFlatMap() throws Exception
    {
        super.testFlatMap();
    }

    @Override
    @Test
    public void
    testForEach() throws Exception
    {
        super.testForEach();
    }

    @Override
    @Test
    public void
    testSinkTo() throws Exception
    {
        super.testSinkTo();
    }

    @Test
    public void
    testKafkaStreams() throws Exception
    {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Long,String> stream =
                builder.stream(
                    "test-topic",
                    Consumed.with(
                        Serdes.serdeFrom(Long.class),
                        Serdes.serdeFrom(String.class)));
        MapUnboundedStreamSink<Long,String> sink =
            new MapUnboundedStreamSink<>(e -> Long.valueOf(e.hashCode()));
        ProcessMethodAccessor<String> accessor =
            new ProcessMethodAccessor<>(sink);

        stream
            .mapValues((k,v) -> v.toUpperCase())
            .foreach((k,v) -> accessor.process(v));

        Topology           topology = builder.build();
        TopologyTestDriver driver = new TopologyTestDriver(topology);
        KafkaStreams       controller = new KafkaStreams(topology,KafkaStreamsPropertiesManager.getProperties());
        TestInputTopic<Long,String> topic =
            driver.createInputTopic(
                "test-topic",
                new LongSerializer(),
                new StringSerializer());

        List
            .of("aaa","bbb","ccc")
            .stream()
            .forEach(e -> topic.pipeInput(Long.valueOf(e.hashCode()),e));


        assertFalse(sink.getStore().isEmpty());
    }

    @Override
    protected IUnboundedStream<String>
    getSubject(String... elements)
    {
        StreamsBuilder                      builder = new StreamsBuilder();
        KafkaUnboundedStream<String,String> stream =
            new KafkaUnboundedStream<>(
                builder.stream(
                    "test-topic",
                    Consumed.with(
                        Serdes.serdeFrom(String.class),
                        Serdes.serdeFrom(String.class))),
                builder);

        driver = new KafkaTestExecutionDriver<>(new StringSerializer(),elements);
        return stream;
    }

    @Override
    protected IExecutionDriver
    getDriver()
    {
        if (driver == null)
            throw
                new IllegalStateException(
                    "Driver has not been initialized. Ensure getSubject() has been called.");

        return driver;
    }
}

//////////////////////////////////////////////////////////////////////////////
