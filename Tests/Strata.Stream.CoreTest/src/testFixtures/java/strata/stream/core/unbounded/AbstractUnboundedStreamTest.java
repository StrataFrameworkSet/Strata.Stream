//////////////////////////////////////////////////////////////////////////////
// AbstractUnboundedStreamTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.unbounded;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import strata.stream.basic.bounded.BasicBoundedStream;
import strata.stream.core.bounded.IBoundedStream;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static strata.foundation.core.concurrent.Awaiter.await;

public abstract
class AbstractUnboundedStreamTest
{
    private IUnboundedStream<String> subject;
    private ChronicleMapUnboundedStreamSink<Long,String> sink;
    private ChronicleMapUnboundedStreamSink<Long,IBoundedStream<String>> sink2;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void
    setUp() throws IOException
    {
        subject =
            getSubject(
                "aaaaa","bbb","foo","cccccccccc","dddddddd","foo");
        sink =
            new ChronicleMapUnboundedStreamSink<>(
                Long.class,
                String.class,
                "chronicle-sink",
                "./build/tmp/chronicle-sink.dat",
                10.0,
                e -> Long.valueOf(e.hashCode()));
        sink2 =
            new ChronicleMapUnboundedStreamSink<>(
                Long.class,
                (Class<IBoundedStream<String>>)BasicBoundedStream.of(List.of("")).getClass(),
                "chronicle-sink2",
                "./build/tmp/chronicle-sink2.dat",
                10.0,
                e -> Long.valueOf(e.hashCode()));
    }

    @AfterEach
    public void
    tearDown()
    {
        if (sink != null && sink.getStore() != null)
        {
            sink.getStore().clear();
            sink.close();
        }

        if (sink2 != null && sink2.getStore() != null)
        {
            sink2.getStore().clear();
            sink2.close();
        }

        sink = null;
        sink2 = null;
        subject = null;
    }

    @Test
    public void
    testKeyBy() throws Exception
    {
        await(
            subject
                .keyBy(e -> e.hashCode() % 5L)
                .map(e -> e.toUpperCase())
                .sinkTo(sink)
                .execute()
                .thenCompose(execution -> execution.getResult()));

        assertTrue(
            sink
                .getStore()
                .values()
                .stream()
                .allMatch(e -> e.toUpperCase().equals(e)));

    }

    @Test
    public void
    testWindowByPlan() throws Exception
    {
        IWindowedUnboundedStream<String> windowed =
            subject.windowBy(
                WindowPlan.ofCountOrDuration(
                    10,
                    Duration.of(5,ChronoUnit.SECONDS)));

        await(
            windowed
                .map(s -> s.map(e -> e.toUpperCase()))
                .sinkTo(sink2)
                .execute(getDriver())
                .thenCompose(execution -> execution.getResult()));

        assertEquals(1,sink2.getStore().size());
    }

    @Test
    public void
    testWindowByDuration() throws Exception
    {
        IWindowedUnboundedStream<String> windowed =
            subject.windowBy(Duration.of(5,ChronoUnit.SECONDS));

        await(
            windowed
                .map(s -> s.map(e -> e.toUpperCase()))
                .sinkTo(sink2)
                .execute(getDriver())
                .thenCompose(execution -> execution.getResult()));

        assertEquals(1,sink2.getStore().size());
    }

    @Test
    public void
    testWindowByCount() throws Exception
    {
        IWindowedUnboundedStream<String> windowed =
            subject.windowBy(6);

        await(
            windowed
                .map(s -> s.map(e -> e.toUpperCase()))
                .sinkTo(sink2)
                .execute(getDriver())
                .thenCompose(execution -> execution.getResult()));

        assertEquals(1,sink2.getStore().size());
    }

    @Test
    public void
    testFilter() throws Exception
    {
        String foo                  = "foo";
        await(
            subject
                .filter(e -> !e.equals(foo))
                .sinkTo(sink)
                .execute(getDriver())
                .thenCompose(execution -> execution.getResult()));

        assertFalse(sink.getStore().isEmpty());
        assertFalse(
            sink
                .getStore()
                .containsValue(foo));
    }

    @Test
    public void
    testMap() throws Exception
    {
        await(
            subject
                .map(e -> e.toUpperCase())
                .sinkTo(sink)
                .execute(getDriver())
                .thenCompose(execution -> execution.getResult()));

        assertFalse(sink.getStore().isEmpty());
        assertTrue(
            sink
                .getStore()
                .values()
                .stream()
                .allMatch(e -> e.toUpperCase().equals(e)));
    }

    @Test
    public void
    testFlatMap() throws Exception
    {
        await(
            subject
                .map(e -> BasicBoundedStream.of(List.of(e.toUpperCase())))
                .flatMap(s -> s.collect(Collectors.toList()))
                .sinkTo(sink)
                .execute(getDriver())
                .thenCompose(execution -> execution.getResult()));


        assertFalse(sink.getStore().isEmpty());
        assertTrue(
            sink
                .getStore()
                .values()
                .stream()
                .allMatch(e -> e.toUpperCase().equals(e)));
    }

    @Test
    public void
    testForEach() throws Exception
    {
        IUnboundedStream<String> other =
            getSubject("aaaaa","bbb","foo","cccccccccc","dddddddd","foo");

        ChronicleMapUnboundedStreamSink<Long,String> otherSink =
            new ChronicleMapUnboundedStreamSink<>(
                Long.class,
                String.class,
                "chronicle-sink",
                "./build/tmp/chronicle-sink.dat",
                10.0,
                e -> Long.valueOf(e.hashCode()));

        ProcessMethodAccessor<String> accessor =
            new ProcessMethodAccessor<>(sink);

        await(
            other
                .map(e -> e.toUpperCase())
                .sinkTo(otherSink)
                .execute(getDriver())
                .thenCompose(execution -> execution.getResult()));

        await(
            subject
                .forEach(e -> accessor.process(e.toUpperCase()))
                .execute(getDriver())
                .thenCompose(execution -> execution.getResult()));

        assertFalse(sink.getStore().isEmpty());
        assertFalse(otherSink.getStore().isEmpty());
        assertIterableEquals(sink.getStore().values(),otherSink.getStore().values());
        otherSink.getStore().clear();
    }

    @Test
    public void
    testSinkTo() throws Exception
    {
         await(
            subject
                .map(e -> e.toUpperCase())
                .sinkTo(sink)
                .execute(getDriver())
                .thenCompose(execution -> execution.getResult()));

        assertFalse(sink.getStore().isEmpty());
        assertTrue(
            sink
                .getStore()
                .values()
                .stream()
                .allMatch(e -> e.toUpperCase().equals(e)));
    }

    protected abstract IUnboundedStream<String>
    getSubject(String... element);

    protected abstract IExecutionDriver
    getDriver();
}

//////////////////////////////////////////////////////////////////////////////
