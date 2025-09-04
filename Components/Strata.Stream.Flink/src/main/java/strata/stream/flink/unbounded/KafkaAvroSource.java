//////////////////////////////////////////////////////////////////////////////
// KafkaInventoryOrderApprovedSource.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Properties;

public
class KafkaAvroSource<T extends SpecificRecord>
    implements Source<T,KafkaPartitionSplit,KafkaSourceEnumState>
{
    private final KafkaSource<T> source;

    public
    KafkaAvroSource(
        String                   hosts,
        String                   topics,
        String                   groupId,
        OffsetsInitializer       startingOffsets,
        DeserializationSchema<T> schema,
        Properties               props)
    {
        source =
            KafkaSource
                .<T>builder()
                .setBootstrapServers(hosts)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(startingOffsets)
                .setValueOnlyDeserializer(schema)
                .setProperties(props)
                .build();
    }

    @Override
    public Boundedness
    getBoundedness()
    {
        return source.getBoundedness();
    }

    @Override
    public SourceReader<T,KafkaPartitionSplit>
    createReader(SourceReaderContext readerContext)
        throws Exception
    {
        return source.createReader(readerContext);
    }

    @Override
    public SplitEnumerator<KafkaPartitionSplit,KafkaSourceEnumState>
    createEnumerator(SplitEnumeratorContext<KafkaPartitionSplit> enumContext)
        throws Exception
    {
        return source.createEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<KafkaPartitionSplit,KafkaSourceEnumState>
    restoreEnumerator(
        SplitEnumeratorContext<KafkaPartitionSplit> enumContext,
        KafkaSourceEnumState                        checkpoint)
        throws Exception
    {
        return source.restoreEnumerator(enumContext,checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<KafkaPartitionSplit>
    getSplitSerializer()
    {
        return source.getSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<KafkaSourceEnumState>
    getEnumeratorCheckpointSerializer()
    {
        return source.getEnumeratorCheckpointSerializer();
    }
}

//////////////////////////////////////////////////////////////////////////////
