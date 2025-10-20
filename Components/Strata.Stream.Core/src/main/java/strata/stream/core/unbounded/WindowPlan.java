/// ///////////////////////////////////////////////////////////////////////////
// WindowPlan.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.unbounded;

import strata.foundation.core.collection.Pair;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;

public
class WindowPlan
{
    private final Pair<Object,Object> plan;

    public
    WindowPlan(Duration primary)
    {
        Objects.requireNonNull(primary);
        plan = Pair.of(primary, null);
    }

    public
    WindowPlan(Duration primary,Integer secondary)
    {
        Objects.requireNonNull(primary);
        plan = Pair.of(primary, secondary);
    }

    public
    WindowPlan(Integer primary)
    {
        Objects.requireNonNull(primary);
        plan = Pair.of(primary, null);
    }

    public
    WindowPlan(Integer primary,Duration secondary)
    {
        Objects.requireNonNull(primary);
        plan = Pair.of(primary, secondary);
    }

    public Pair<Duration,Integer>
    getDurationOrCount()
    {
        return
            Pair.of(
                getPrimary(Duration.class),
                getSecondary(Integer.class).orElse(500));
    }

    public Pair<Integer,Duration>
    getCountOrDuration()
    {
        return
            Pair.of(
                getPrimary(Integer.class),
                getSecondary(Duration.class).orElse(Duration.ofSeconds(30)));
    }

    public <T> T
    getPrimary(Class<T> type)
        throws IllegalArgumentException,ClassCastException
    {
        validateType(type);

        return type.cast(plan.getFirst());
    }

    public <T> Optional<T>
    getSecondary(Class<T> type)
        throws IllegalArgumentException
    {
        validateType(type);

        try
        {
            return Optional.ofNullable(type.cast(plan.getSecond()));
        }
        catch (ClassCastException ex)
        {
            return Optional.empty();
        }
    }

    public <T> boolean
    isPrimary(Class<T> type)
        throws IllegalArgumentException
    {
        validateType(type);
        return type.isInstance(plan.getFirst());
    }

    public <T> boolean
    isSecondary(Class<T> type)
        throws IllegalArgumentException
    {
        validateType(type);
        return type.isInstance(plan.getSecond());
    }

    public boolean
    hasSecondary()
    {
        return Objects.nonNull(plan.getSecond());
    }

    public static WindowPlan
    ofDuration(Duration duration)
    {
        return new WindowPlan(duration);
    }

    public static WindowPlan
    ofDuration(long amount,ChronoUnit unit)
    {
        return new WindowPlan(Duration.of(amount, unit));
    }

    public static WindowPlan
    ofDurationOrCount(Duration duration,int count)
    {
        return new WindowPlan(duration,count);
    }

    public static WindowPlan
    ofDurationOrCount(long amount,ChronoUnit unit,int maxSize)
    {
        return new WindowPlan(Duration.of(amount, unit),maxSize);
    }

    public static WindowPlan
    ofCount(int count)
    {
        return new WindowPlan(count);
    }

    public static WindowPlan
    ofCountOrDuration(int maxSize,Duration duration)
    {
        return new WindowPlan(maxSize,duration);
    }

    public static WindowPlan
    ofCountOrDuration(int maxSize,long amount,ChronoUnit unit)
    {
        return new WindowPlan(maxSize,Duration.of(amount, unit));
    }

    private <T> void
    validateType(Class<T> type)
        throws IllegalArgumentException
    {
        if (!type.equals(Duration.class) && !type.equals(Integer.class))
            throw
                new IllegalArgumentException("Unsupported window type: " + type.getName());
    }
}

//////////////////////////////////////////////////////////////////////////////
