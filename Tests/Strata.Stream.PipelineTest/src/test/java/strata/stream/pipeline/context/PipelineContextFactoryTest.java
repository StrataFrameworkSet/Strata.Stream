//////////////////////////////////////////////////////////////////////////////
// PipelineContextFactoryTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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
            new ToUpperContextFactory()
                .create(
                    toString
                        .startStep(".Next")
                        .completeStep(),"ONE");

        assertEquals("one",toString.getValue());
        assertEquals(2,toString.getAccumulatedSteps().size());
        assertEquals(
            StepStatus.COMPLETED,
            toString
                .getAccumulatedSteps()
                .getLast()
                .getStatus());

        assertEquals("ONE",toUpper.getValue());
        assertEquals(2,toUpper.getAccumulatedSteps().size());
        assertEquals(
            StepStatus.COMPLETED,
            toUpper
                .getAccumulatedSteps()
                .getLast()
                .getStatus());
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

        assertFalse(toString.hasValue());
        assertNull(toString.getValue());
        assertEquals(2,toString.getAccumulatedSteps().size());
        assertEquals(
            StepStatus.FAILED,
            toString
                .getAccumulatedSteps()
                .getLast()
                .getStatus());

        assertFalse(toUpper.hasValue());
        assertNull(toUpper.getValue());
        assertEquals(3,toUpper.getAccumulatedSteps().size());
        assertEquals(
            StepStatus.FAILED,
            toUpper
                .getAccumulatedSteps()
                .getLast()
                .getStatus());
    }

}

//////////////////////////////////////////////////////////////////////////////
