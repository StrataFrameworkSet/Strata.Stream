//////////////////////////////////////////////////////////////////////////////
// AbstractBoundedStreamTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.bounded;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import strata.stream.basic.bounded.BasicBoundedStream;
import strata.stream.basic.bounded.ListBoundedStreamSink;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public abstract
class AbstractBoundedStreamTest
{
    IBoundedStream<String> subject;

    @BeforeEach
    public void
    setUp()
    {
        subject =
            getSubject("aaaaa","bbb","foo","cccccccccc","dddddddd","foo");
    }

    @AfterEach
    public void
    tearDown()
    {
        subject = null;
    }

    @Test
    public void
    testFilter() throws Exception
    {
        String foo                  = "foo";
        ListBoundedStreamSink<String> sink = new ListBoundedStreamSink<>();

        subject
            .filter(e -> !e.equals(foo))
            .sinkTo(sink);

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

        subject
            .map(e -> e.toUpperCase())
            .sinkTo(sink);

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

        subject
            .map(e -> BasicBoundedStream.of(List.of(e.toUpperCase())))
            .flatMap(stream -> stream.collect(Collectors.toList()))
            .sinkTo(sink);

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
        IBoundedStream<String> other = getSubject("aaaaa","bbb","foo","cccccccccc","dddddddd","foo");
        ListBoundedStreamSink<String> sink = new ListBoundedStreamSink<>();
        List<String>           list = new ArrayList<>();

        other
            .map(e -> e.toUpperCase())
            .sinkTo(sink);

        subject.forEach(e -> list.add(e.toUpperCase()));

        assertFalse(sink.getStore().isEmpty());
        assertIterableEquals(sink.getStore(),list);
    }

    @Test
    public void
    testSinkTo() throws Exception
    {
        ListBoundedStreamSink<String> sink = new ListBoundedStreamSink<>();

        subject
            .map(e -> e.toUpperCase())
            .sinkTo(sink);

        assertFalse(sink.getStore().isEmpty());
        assertTrue(
            sink
                .getStore()
                .stream()
                .allMatch(e -> e.toUpperCase().equals(e)));
    }

    protected abstract IBoundedStream<String>
    getSubject(String... elements);
}

//////////////////////////////////////////////////////////////////////////////
