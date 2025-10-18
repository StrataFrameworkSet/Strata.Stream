//////////////////////////////////////////////////////////////////////////////
// KafkaTestExecutionDriver.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.core.unbounded.IExecutionDriver;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;

@SuppressWarnings("unchecked")
public
class KafkaTestExecutionDriver<T>
    implements IExecutionDriver
{
    private final Serializer<T> serializer;
    private final List<T> sequence;
    private final Logger  logger;

    public KafkaTestExecutionDriver(Serializer<T> serializer,T... elements)
    {
        this.serializer = serializer;
        this.sequence = List.of(elements);
        this.logger = LogManager.getLogger(getClass());
    }

    @Override
    public <I> IStreamExecution
    start(I input)
    {
        if (input instanceof Topology topology)
        {
            logger.info("Starting Kafka test driver with topology: {}",topology);
            TopologyTestDriver driver =
                new TopologyTestDriver(topology,getDefaultProperties());
            TestInputTopic<String,T> topic =
                driver.createInputTopic(
                    "test-topic",
                    new StringSerializer(),
                    serializer);

            logger.info("Publishing {} elements to test topic...",sequence.size());
            sequence.forEach(element -> topic.pipeInput("key",element));

            logger.info("Advancing clock by 10 seconds...");
            driver.advanceWallClockTime(Duration.ofSeconds(10));

            return new KafkaStreamExecution(topology);
        }

        throw new IllegalArgumentException(
            "Unsupported input type: " + input.getClass().getName());
    }

    private Properties
    getDefaultProperties()
    {
        Properties props = new Properties();
        props.put("application.id","test-app");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());


        // Enable detailed logging
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG,"/tmp/kafka-streams-test");

        // Add metrics and debugging
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG,"DEBUG");
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG,StreamsConfig.OPTIMIZE);

        return props;
    }

    private void
    advanceClock(TopologyTestDriver driver,int secondTicks)
    {
        for (int i = 0; i < secondTicks; i++)
        {
            logger.info("Advancing clock by one second...");
            driver.advanceWallClockTime(Duration.ofSeconds(1));
            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}

//////////////////////////////////////////////////////////////////////////////
