//////////////////////////////////////////////////////////////////////////////
// FlinkUnboundedStreamTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import strata.stream.core.bounded.IBoundedStream;
import strata.stream.core.shared.IFunction;
import strata.stream.core.unbounded.*;

import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static strata.foundation.core.concurrent.Awaiter.await;

@Tag("CommitStage")
public
class FlinkUnboundedStreamTest
    extends AbstractUnboundedStreamTest
{
    @BeforeAll
    public static void
    registerFlatMapReturnTypes()
    {
        FlinkFlatMapFunctionAdapter.registerReturnType(
            (IFunction<IBoundedStream<String>,Iterable<String>>)s -> s.collect(Collectors.toList()),
            String.class);
    }

    @Test
    public void
    testFlink() throws Exception
    {
        StreamExecutionEnvironment environment =
            StreamExecutionEnvironment.createLocalEnvironment();
        ChronicleMapUnboundedStreamSink<Long,String> sink =
            new ChronicleMapUnboundedStreamSink<>(
                Long.class,
                String.class,
                "chronicle-sink-a",
                "./build/tmp/chronicle-sink-a.dat",
                10.0,
                e -> Long.valueOf(e.hashCode()));

        environment
            .fromData("aaaa","bbbb","cccc","foo")
            .filter(e -> !e.equals("foo"))
            .map(e -> e.toUpperCase())
            .sinkTo(new SinkAdapter<>(sink));

        JobClient client = environment.executeAsync();

        await(client.getJobExecutionResult());

        assertFalse(sink.getStore().isEmpty());
        assertTrue(
            sink
                .getStore()
                .values()
                .stream()
                .allMatch(e -> e.toUpperCase().equals(e)));

        sink.getStore().clear();
        sink.close();
    }

    @Test
    public void
    testFlatMap() throws Exception
    {
        super.testFlatMap();
    }

    @Test
    public void
    testForEach() throws Exception
    {
        super.testForEach();
    }


    @Test
    public void
    testForEachAgain() throws Exception
    {
        IUnboundedStream<String> stream =
            getSubject("aaaaa","bbb","foo","cccccccccc","dddddddd","foo");

        await(
            stream
                .forEach(e -> System.out.println(e))
                .execute(getDriver())
                .thenCompose(execution -> execution.getResult()));
    }

    /*
        @Test
        public void
        testFilter() throws Exception
        {
            String foo                  = "foo";
            ListBoundedStreamSink<String> sink = new ListBoundedStreamSink<>();

            execute(
                subject
                    .filter(e -> !e.equals(foo))
                    .sinkTo(sink));

            assertFalse(sink.getStore().isEmpty());
            assertFalse(
                sink
                    .getStore()
                    .contains(foo));
        }

        @Test
        public void
        testMap() throws Exception
        {
            ListBoundedStreamSink<String> sink = new ListBoundedStreamSink<>();

            execute(
                subject
                    .map(e -> e.toUpperCase())
                    .sinkTo(sink));

            assertFalse(sink.getStore().isEmpty());
            assertTrue(
                sink
                    .getStore()
                    .stream()
                    .allMatch(e -> e.toUpperCase().equals(e)));
        }

        @Test
        public void
        testFlatMap() throws Exception
        {
            ListBoundedStreamSink<String> sink = new ListBoundedStreamSink<>();

            execute(
                subject
                    .flatMap(e -> BasicBoundedStream.of(List.of(e.toUpperCase())))
                    .sinkTo(sink));

            assertFalse(sink.getStore().isEmpty());
            assertTrue(
                sink
                    .getStore()
                    .stream()
                    .allMatch(e -> e.toUpperCase().equals(e)));
        }

        @Test
        public void
        testForEach() throws Exception
        {
            IStream<String> other = getSubject("aaaaa","bbb","foo","cccccccccc","dddddddd","foo");
            ListBoundedStreamSink<String> sink = new ListBoundedStreamSink<>();
            List<String>           list = new ArrayList<>();

            execute(
                other
                    .map(e -> e.toUpperCase())
                    .sinkTo(sink));

            execute(subject.forEach(e -> list.add(e.toUpperCase())));

            assertFalse(sink.getStore().isEmpty());
            assertIterableEquals(sink.getStore(),list);
        }

        @Test
        public void
        testSinkTo() throws Exception
        {
            ListBoundedStreamSink<String> sink = new ListBoundedStreamSink<>();

            execute(
                subject
                    .map(e -> e.toUpperCase())
                    .sinkTo(sink));

            assertFalse(sink.getStore().isEmpty());
            assertTrue(
                sink
                    .getStore()
                    .stream()
                    .allMatch(e -> e.toUpperCase().equals(e)));
        }
    */
    @Override
    protected IUnboundedStream<String>
    getSubject(String... elements)
    {
        return new FromElementsUnboundedStreamSource<>(String.class,elements);
    }

    @Override
    protected IExecutionDriver
    getDriver()
    {
        return new FlinkExecutionDriver();
    }
}

//////////////////////////////////////////////////////////////////////////////
