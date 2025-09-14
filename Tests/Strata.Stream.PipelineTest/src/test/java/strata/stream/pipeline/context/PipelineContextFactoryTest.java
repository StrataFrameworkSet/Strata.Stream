/// ///////////////////////////////////////////////////////////////////////////
// PipelineContextFactoryTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("CommitStage")
public
class PipelineContextFactoryTest
{
    @Test
    public void
    testCreateContextWithValue()
    {
        InitialContext initial =
            InitialContext
                .of(1L)
                .startStep(".A")
                .completeStep();
        ToStringContext toString =
            new ToStringContextFactory().create(initial,"one");
        ToUpperContext toUpper =
            new ToUpperContextFactory().create(toString.completeStep(),"ONE");

        assertEquals("one",toString.getValue());
        assertEquals(2,toString.getAccumulatedResults().size());
        assertEquals(StepStatus.COMPLETED,toString.getStatus());

        assertEquals("ONE",toUpper.getValue());
        assertEquals(3,toUpper.getAccumulatedResults().size());
        assertEquals(StepStatus.IN_PROGRESS,toUpper.getStatus());
    }
    @Test
    public void
    testCreateContextWithException()
    {
        InitialContext initial =
            InitialContext
                .of(1L)
                .startStep(".A")
                .completeStep();
        ToStringContext toString =
            new ToStringContextFactory().create(initial,new TestException());
        ToUpperContext toUpper =
            new ToUpperContextFactory().create(toString,new TestException());

        assertThrows(
            NullPointerException.class,
            () -> toString.getValue());
        assertEquals(2,toString.getAccumulatedResults().size());
        assertEquals(StepStatus.FAILED,toString.getStatus());

        assertThrows(
            NullPointerException.class,
            () -> toUpper.getValue());
        assertEquals(3,toUpper.getAccumulatedResults().size());
        assertEquals(StepStatus.FAILED,toUpper.getStatus());
    }

}

//////////////////////////////////////////////////////////////////////////////
