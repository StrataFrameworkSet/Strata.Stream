//////////////////////////////////////////////////////////////////////////////
// TimeAmount.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.shared;

import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public
class TimeAmount
    implements Serializable
{
    private final long     size;
    private final TimeUnit units;

    public
    TimeAmount(long s,TimeUnit u)
    {
        size  = s;
        units = u;
    }

    public long
    getSize() { return size; }

    public TimeUnit
    getUnits() { return units; }

    public Duration
    toDuration() { return Duration.of(size,units.toChronoUnit()); }

    public static TimeAmount
    of(long size,TimeUnit units)
    {
        return new TimeAmount(size,units);
    }
}

//////////////////////////////////////////////////////////////////////////////
