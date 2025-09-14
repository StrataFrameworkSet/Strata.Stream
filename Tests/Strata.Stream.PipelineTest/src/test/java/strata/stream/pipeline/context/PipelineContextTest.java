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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
                List.of(StepResult.of("Initial",StepStatus.COMPLETED)));
        toUpper =
            new ToUpperContext(
                "SEVEN",
                List.of(
                    StepResult.of("Initial",StepStatus.COMPLETED),
                    StepResult.of("ToString",StepStatus.COMPLETED)));
    }

    @Test
    public void
    testConstructors()
    {
        assertEquals("Initial",initial.getStep());
        assertEquals(7L,initial.getValue());
        assertEquals(StepStatus.IN_PROGRESS,initial.getStatus());
        assertEquals(1,initial.getAccumulatedResults().size());

        assertEquals("ToString",toString.getStep());
        assertEquals("Seven",toString.getValue());
        assertEquals(StepStatus.IN_PROGRESS,toString.getStatus());
        assertEquals(2,toString.getAccumulatedResults().size());

        assertEquals("ToUpper",toUpper.getStep());
        assertEquals("SEVEN",toUpper.getValue());
        assertEquals(StepStatus.IN_PROGRESS,toUpper.getStatus());
        assertEquals(3,toUpper.getAccumulatedResults().size());

        toString =
            new ToStringContext(
                new SyntacticValidationFailedException(),
                List.of(StepResult.of("Initial",StepStatus.COMPLETED)));
        assertEquals("ToString",toString.getStep());
        assertEquals(StepStatus.FAILED,toString.getStatus());

        toUpper =
            new ToUpperContext(
                "SEVEN",
                List.of(
                    StepResult.of("Initial",StepStatus.COMPLETED),
                    StepResult.of(
                        "ToString",
                        StepStatus.FAILED,
                        new SyntacticValidationFailedException())));

        assertEquals("ToUpper",toUpper.getStep());
        assertEquals(StepStatus.FAILED,toUpper.getStatus());
    }

    @Test
    public void
    testStartStep()
    {
        assertEquals(
            "ToUpperNextStep",
            toUpper
                .startStep("NextStep")
                .getStep());
    }


    @Test
    public void
    testCompleteStep()
    {
        assertEquals(
            "Initial.NextStep",
            initial
                .startStep(".NextStep")
                .getStep());
        assertEquals(
            "Initial",
            initial
                .completeStep()
                .getStep());
        assertEquals(
            StepStatus.COMPLETED,
            initial.getStatus());
        assertEquals(
            StepStatus.COMPLETED,
            initial
                .completeStep()
                .getStatus());
        assertThrows(
            IllegalStateException.class,
            () ->
                initial
                    .completeStepWith(new SyntacticValidationFailedException())
                    .getStatus());
        assertThrows(
            IllegalStateException.class,
            () ->
                initial
                    .failStepWith(new SyntacticValidationFailedException())
                    .getStatus());
    }

    @Test
    public void
    testCompleteStepWith()
    {
        assertEquals(
            "Initial.NextStep",
            initial
                .startStep(".NextStep")
                .getStep());
        assertEquals(
            "Initial",
            initial
                .completeStepWith(new SyntacticValidationFailedException())
                .getStep());
        assertEquals(
            StepStatus.COMPLETED_WITH_EXCEPTION,
            initial.getStatus());
        assertEquals(
            StepStatus.COMPLETED_WITH_EXCEPTION,
            initial
                .completeStepWith(new SyntacticValidationFailedException())
                .getStatus());
        assertThrows(
            IllegalStateException.class,
            () ->
                initial
                    .completeStep()
                    .getStatus());
        assertThrows(
            IllegalStateException.class,
            () ->
                initial
                    .failStepWith(new SyntacticValidationFailedException())
                    .getStatus());
    }

    @Test
    public void
    testFailStepWith()
    {
        assertEquals(
            "Initial.NextStep",
            initial
                .startStep(".NextStep")
                .getStep());
        assertEquals(
            "Initial",
            initial
                .failStepWith(new SyntacticValidationFailedException())
                .getStep());
        assertEquals(
            StepStatus.FAILED,
            initial.getStatus());
        assertEquals(
            StepStatus.FAILED,
            initial
                .failStepWith(new SyntacticValidationFailedException())
                .getStatus());
        assertThrows(
            IllegalStateException.class,
            () ->
                initial
                    .completeStep()
                    .getStatus());
        assertThrows(
            IllegalStateException.class,
            () ->
                initial
                    .completeStepWith(new SyntacticValidationFailedException())
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
                    StepResult.of("Initial",StepStatus.COMPLETED),
                    StepResult.of("ToString",StepStatus.COMPLETED)));
        assertEquals("Seven", toUpper.getValue());
        assertEquals(
            Conditional.TRUE,
            toUpper.recover(new SyntacticValidationFailedException(Set.of(1234))));
        assertEquals("SEVEN", toUpper.getValue());

        toUpper =
            new ToUpperContext(
                "Seven",
                List.of(
                    StepResult.of("Initial",StepStatus.COMPLETED),
                    StepResult.of("ToString",StepStatus.COMPLETED)));
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
