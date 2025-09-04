//////////////////////////////////////////////////////////////////////////////
// TimestampAssigner.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

import java.time.Instant;

public
class TimestampAssigner<T>
    implements SerializableTimestampAssigner<T>
{
    @Override
    public long
    extractTimestamp(T element,long recordTimestamp)
    {
        return
            Instant
                .now()
                .toEpochMilli();
    }
}

//////////////////////////////////////////////////////////////////////////////
