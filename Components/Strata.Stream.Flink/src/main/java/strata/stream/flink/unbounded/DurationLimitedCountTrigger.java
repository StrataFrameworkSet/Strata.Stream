//////////////////////////////////////////////////////////////////////////////
// DurationLimitedCountTrigger.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public
class DurationLimitedCountTrigger<T>
    extends Trigger<T,TimeWindow>
{
    private static final long serialVersionUID = 1L;

    private final int maxCount;

    public
    DurationLimitedCountTrigger(int maxCount)
    {
        this.maxCount = maxCount;
    }

    @Override
    public TriggerResult
    onElement(T element,long timestamp,TimeWindow window,TriggerContext ctx)
        throws Exception
    {
        ValueState<Long> countState =
            ctx.getPartitionedState(
                new ValueStateDescriptor<>("count", Types.LONG));

        Long count = countState.value();

        if (count == null)
            count = 0L;

        count++;
        countState.update(count);

        ctx.registerEventTimeTimer(window.maxTimestamp());

        if (count >= maxCount)
        {
            countState.clear();
            return TriggerResult.FIRE_AND_PURGE;
        }

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult
    onEventTime(long time,TimeWindow window,TriggerContext ctx)
        throws Exception
    {
        return
            time == window.maxTimestamp()
                ? TriggerResult.FIRE_AND_PURGE
                : TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult
    onProcessingTime(long time,TimeWindow window,TriggerContext ctx)
    {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void
    clear(TimeWindow window,TriggerContext ctx)
        throws Exception
    {
        ctx.getPartitionedState(
            new ValueStateDescriptor<>("count", Types.LONG)).clear();
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    public static <T> DurationLimitedCountTrigger<T>
    of(int maxCount)
    {
        return new DurationLimitedCountTrigger<>(maxCount);
    }
}

//////////////////////////////////////////////////////////////////////////////
