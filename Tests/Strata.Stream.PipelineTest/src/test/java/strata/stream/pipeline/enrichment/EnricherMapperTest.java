//////////////////////////////////////////////////////////////////////////////
// EnricherMapperTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.enrichment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import strata.stream.pipeline.context.InitialContext;
import strata.stream.pipeline.context.ToStringContext;
import strata.stream.pipeline.context.ToUpperContext;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("CommitStage")
public
class EnricherMapperTest
{
    private IInitialContextToStringContextEnricherMapper enricherA;
    private IToStringContextToUpperContextEnricherMapper enricherB;

    @BeforeEach
    public void
    setup()
    {
        enricherA =
            new InitialContextToStringContextEnricherMapper(
                (input) -> "Value=" + input);
        enricherB = new ToStringContextToUpperContextEnricherMapper(
            (input) -> input.toUpperCase());
    }

    @Test
    public void
    testApply()
    {
        InitialContext  initial  = InitialContext.of(1L);
        ToStringContext toString = enricherA.apply(initial);
        ToUpperContext  toUpper  = enricherB.apply(toString);

        assertEquals("Value=1",toString.getValue());
        assertEquals(2,toString.getAccumulatedResults().size());
        assertEquals("VALUE=1",toUpper.getValue());
        assertEquals(3,toUpper.getAccumulatedResults().size());
    }
}

//////////////////////////////////////////////////////////////////////////////
