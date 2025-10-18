//////////////////////////////////////////////////////////////////////////////
// BasicUnboundedStreamTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.unbounded;

import org.junit.jupiter.api.Tag;
import strata.stream.core.unbounded.AbstractUnboundedStreamTest;
import strata.stream.core.unbounded.IExecutionDriver;
import strata.stream.core.unbounded.IUnboundedStream;

//@Tag("CommitStage")
public
class BasicUnboundedStreamTest
    extends AbstractUnboundedStreamTest
{
    @Override
    protected IUnboundedStream<String>
    getSubject(String... elements)
    {
        return null;
    }

    @Override
    protected IExecutionDriver
    getDriver()
    {
        return null;
    }
}

//////////////////////////////////////////////////////////////////////////////
