/// ///////////////////////////////////////////////////////////////////////////
// PipelineContextTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import strata.foundation.core.utility.Conditional;
import strata.stream.pipeline.validation.SyntacticValidationFailedException;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@Tag("CommitStage")
public
class PipelineContextTest
{
    private InitialContext  initial;
    private ToStringContext toString;
    private ToUpperContext  toUpper;

    @BeforeEach
    public void
    setup()
    {
        initial = new InitialContext(7L);
        toString =
            new ToStringContext(
                "Seven",
                List.of(PipelineStep.of(initial,"Initial",StepStatus.COMPLETED)));
        toUpper =
            new ToUpperContext(
                "SEVEN",
                List.of(
                    PipelineStep.of(toString,"ToString",StepStatus.COMPLETED),
                    PipelineStep.of(toString,"ToString",StepStatus.COMPLETED)));
    }

    @Test
    public void
    testConstructors()
    {
        assertTrue(initial.getCurrentStep().isEmpty());
        assertTrue(initial.getCurrentStep().isEmpty());
        assertEquals(7L,initial.getValue());
        assertEquals(0,initial.getAccumulatedSteps().size());
        initial.startStep(".NextStep");
        assertEquals(1,initial.getAccumulatedSteps().size());

        assertTrue(toString.getCurrentStep().isEmpty());
        assertEquals("Seven",toString.getValue());
        assertEquals(1,toString.getAccumulatedSteps().size());
        toString.startStep(".NextStep");
        assertEquals(2,toString.getAccumulatedSteps().size());


        assertTrue(toUpper.getCurrentStep().isEmpty());
        assertEquals("SEVEN",toUpper.getValue());
        assertEquals(2,toUpper.getAccumulatedSteps().size());

        toString =
            new ToStringContext(
                new SyntacticValidationFailedException(),
                List.of(PipelineStep.of(initial,"Initial",StepStatus.COMPLETED)));
        assertTrue(toString.getCurrentStep().isPresent());
        assertEquals(
            StepStatus.FAILED,
            toString
                .getCurrentStep()
                .orElseThrow()
                .getStatus());

        toUpper =
            new ToUpperContext(
                "SEVEN",
                List.of(
                    PipelineStep.of(initial,"Initial",StepStatus.COMPLETED),
                    PipelineStep.of(
                        toString,
                        "ToString",
                        new SyntacticValidationFailedException())));

        assertTrue(toUpper.getCurrentStep().isEmpty());
        assertEquals(
            StepStatus.FAILED,
            toUpper
                .getAccumulatedSteps()
                .getLast()
                .getStatus());
    }

    @Test
    public void
    testStartStep()
    {
        assertEquals(
            "ToUpperNextStep",
            toUpper
                .startStep("NextStep")
                .getCurrentStep()
                .orElseThrow()
                .getName());
    }


    @Test
    public void
    testCompleteStep()
    {
        assertEquals(
            "Initial.NextStep",
            initial
                .startStep(".NextStep")
                .getCurrentStep()
                .orElseThrow()
                .getName());
        assertEquals(
            "Initial.NextStep",
            initial
                .completeStep()
                .getAccumulatedSteps()
                .getLast()
                .getName());
        assertEquals(
            StepStatus.COMPLETED,
            initial
                .getAccumulatedSteps()
                .getLast()
                .getStatus());
        assertEquals(
            StepStatus.COMPLETED,
            initial
                .startStep(".NextStep")
                .completeStep()
                .getAccumulatedSteps()
                .getLast()
                .getStatus());
        assertThrows(
            IllegalStateException.class,
            () ->
                initial
                    .startStep(".NextStep")
                    .completeStepWith(new SyntacticValidationFailedException())
                    .completeStep());
        assertThrows(
            IllegalStateException.class,
            () ->
                initial
                    .failStepWith(new SyntacticValidationFailedException()));
    }

    @Test
    public void
    testCompleteStepWith()
    {
        assertEquals(
            "Initial.NextStep",
            initial
                .startStep(".NextStep")
                .getCurrentStep()
                .orElseThrow()
                .getName());
        assertEquals(
            "Initial.NextStep",
            initial
                .completeStepWith(new SyntacticValidationFailedException())
                .getAccumulatedSteps()
                .getLast()
                .getName());
        assertEquals(
            StepStatus.COMPLETED_WITH_EXCEPTION,
            initial
                .getAccumulatedSteps()
                .getLast()
                .getStatus());
        assertEquals(
            StepStatus.COMPLETED_WITH_EXCEPTION,
            initial
                .startStep(".NextStep")
                .completeStepWith(new SyntacticValidationFailedException())
                .getAccumulatedSteps()
                .getLast()
                .getStatus());
        assertThrows(
            IllegalStateException.class,
            () ->
                initial
                    .startStep(".NextStep")
                    .completeStep()
                    .failStepWith(new SyntacticValidationFailedException()));
        assertThrows(
            IllegalStateException.class,
            () ->
                initial
                    .startStep(".NextStep")
                    .failStepWith(new SyntacticValidationFailedException())
                    .completeStepWith(new SyntacticValidationFailedException()));
    }

    @Test
    public void
    testFailStepWith()
    {
        assertEquals(
            "Initial.NextStep",
            initial
                .startStep(".NextStep")
                .getCurrentStep()
                .orElseThrow()
                .getName());
        assertEquals(
            "Initial.NextStep",
            initial
                .failStepWith(new SyntacticValidationFailedException())
                .getAccumulatedSteps()
                .getLast()
                .getName());
        assertEquals(
            StepStatus.FAILED,
            initial
                .getAccumulatedSteps()
                .getLast()
                .getStatus());
        assertEquals(
            StepStatus.FAILED,
            initial
                .startStep(".NextStep")
                .failStepWith(new SyntacticValidationFailedException())
                .getAccumulatedSteps()
                .getLast()
                .getStatus());
        assertThrows(
            IllegalStateException.class,
            () ->
                initial
                    .startStep(".NextStep")
                    .startStep(".AnotherStep")
                    .completeStep()
                    .getAccumulatedSteps()
                    .getLast()
                    .getStatus());
        assertThrows(
            IllegalStateException.class,
            () ->
                initial
                    .startStep(".NextStep")
                    .completeStepWith(new SyntacticValidationFailedException())
                    .completeStep()
                    .getAccumulatedSteps()
                    .getLast()
                    .getStatus());
    }

    @Test
    public void
    testRecover()
    {
        assertEquals(
            Conditional.FALSE,
            initial.recover(new SyntacticValidationFailedException()));
        assertEquals(
            Conditional.FALSE,
            initial.recover(new TestException()));

        assertEquals(
            Conditional.FALSE,
            toString.recover(new SyntacticValidationFailedException()));
        assertEquals(
            Conditional.TRUE,
            toString.recover(new TestException()));

        toUpper =
            new ToUpperContext(
                "Seven",
                List.of(
                    PipelineStep.of(initial,"Initial",StepStatus.COMPLETED),
                    PipelineStep.of(toString,"ToString",StepStatus.COMPLETED)));
        assertEquals("Seven", toUpper.getValue());
        assertEquals(
            Conditional.TRUE,
            toUpper.recover(new SyntacticValidationFailedException(Set.of(1234))));
        assertEquals("SEVEN", toUpper.getValue());

        toUpper =
            new ToUpperContext(
                "Seven",
                List.of(
                    PipelineStep.of(initial,"Initial",StepStatus.COMPLETED),
                    PipelineStep.of(toString,"ToString",StepStatus.COMPLETED)));
        assertEquals("Seven", toUpper.getValue());
        assertEquals(
            Conditional.FALSE,
            toUpper.recover(new SyntacticValidationFailedException(Set.of(5678))));
        assertEquals("Seven", toUpper.getValue());
        assertEquals(
            Conditional.FALSE,
            toUpper.recover(new SyntacticValidationFailedException()));
        assertEquals("Seven", toUpper.getValue());

        assertEquals(
            Conditional.FALSE,
            toUpper.recover(new TestException()));
        assertEquals("Seven", toUpper.getValue());
    }

    @Test
    public void
    testIsRecoverable()
    {
        assertEquals(
            false,
            initial.isRecoverable(new SyntacticValidationFailedException()));
        assertEquals(
            false,
            initial.isRecoverable(new SyntacticValidationFailedException()));
        assertEquals(
            false,
            initial.isRecoverable(new TestException()));

        assertEquals(
            false,
            toString.isRecoverable(new SyntacticValidationFailedException()));
        assertEquals(
            false,
            toString.isRecoverable(new SyntacticValidationFailedException()));
        assertEquals(
            true,
            toString.isRecoverable(new TestException()));


        assertEquals(
            true,
            toUpper.isRecoverable(new SyntacticValidationFailedException(Set.of(1234))));
        assertEquals(
            false,
            toUpper.isRecoverable(new SyntacticValidationFailedException(Set.of(5678))));
        assertEquals(
        false,
        toUpper.isRecoverable(new SyntacticValidationFailedException()));
        assertEquals(
            false,
            toUpper.isRecoverable(new TestException()));

    }

}

//////////////////////////////////////////////////////////////////////////////
