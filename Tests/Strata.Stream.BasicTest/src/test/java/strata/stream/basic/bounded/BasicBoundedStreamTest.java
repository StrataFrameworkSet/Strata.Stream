//////////////////////////////////////////////////////////////////////////////
// BasicBoundedStreamTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.bounded;

import org.junit.jupiter.api.Tag;
import strata.stream.core.bounded.AbstractBoundedStreamTest;
import strata.stream.core.bounded.IBoundedStream;

import java.util.List;

@Tag("CommitStage")
public
class BasicBoundedStreamTest
    extends AbstractBoundedStreamTest
{
    @Override
    protected IBoundedStream<String>
    getSubject(String... elements)
    {
        return BasicBoundedStream.of(List.of(elements));
    }
}

//////////////////////////////////////////////////////////////////////////////
