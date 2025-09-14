/// ///////////////////////////////////////////////////////////////////////////
// EnricherMapperTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.transformation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import strata.stream.pipeline.context.InitialContext;
import strata.stream.pipeline.context.ToStringContext;
import strata.stream.pipeline.context.ToUpperContext;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("CommitStage")
public
class TransformerMapperTest
{
    private IInitialContextToStringContextTransformerMapper transformerA;
    private IToStringContextToUpperContextTransformerMapper transformerB;

    @BeforeEach
    public void
    setup()
    {
        transformerA =
            new InitialContextToStringContextTransformerMapper(
                (input) -> "Value=" + input);
        transformerB = new ToStringContextToUpperContextTransformerMapper(
            (input) -> input.toUpperCase());
    }

    @Test
    public void
    testApply()
    {
        InitialContext  initial  = InitialContext.of(1L);
        ToStringContext toString = transformerA.apply(initial);
        ToUpperContext  toUpper  = transformerB.apply(toString);

        assertEquals("Value=1",toString.getValue());
        assertEquals(2,toString.getAccumulatedResults().size());
        assertEquals("VALUE=1",toUpper.getValue());
        assertEquals(3,toUpper.getAccumulatedResults().size());
    }
}

//////////////////////////////////////////////////////////////////////////////
