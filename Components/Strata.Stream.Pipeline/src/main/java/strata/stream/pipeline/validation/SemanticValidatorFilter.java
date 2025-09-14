//////////////////////////////////////////////////////////////////////////////
// SemanticValidatorFilter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import strata.stream.pipeline.context.IPipelineContext;

import java.util.function.Predicate;

public
class SemanticValidatorFilter<T,C extends IPipelineContext<T>>
    implements ISemanticValidatorFilter<T,C>
{
    private final ISemanticValidator<T> validator;

    public
    SemanticValidatorFilter(ISemanticValidator<T> validator)
    {
        this.validator = validator;
    }

    @Override
    public boolean
    test(C context)
    {
        context.startStep(".ValidateSemantics");

        try
        {
            doValidate(context);
            return true;
        }
        catch (SemanticValidationFailedException e)
        {
           if (context.isRecoverable(e))
               return doRecovery(context,e);

              return createFailedValidation(context,e);
        }
        catch (Throwable cause)
        {
            return
                createFailedValidation(
                    context,
                    new SemanticValidationFailedException(cause));
        }
    }

    private void
    doValidate(C context) throws SemanticValidationFailedException
    {
        validator.validate(context.getValue());
        context.completeStep();
    }


    private boolean
    doRecovery(C context,SemanticValidationFailedException exception)
    {
        try
        {
            return
                context
                    .recover(exception)
                    .ifTrueOrElse(
                        () -> reTest(context),
                        () -> createFailedValidation(context,exception));
        }
        catch (Throwable cause)
        {
            return
                createFailedValidation(
                    context,
                    new SemanticValidationFailedException(cause));
        }
    }

    private boolean
    reTest(C context)
    {
        try
        {
            doValidate(context);
            return true;
        }
        catch (SemanticValidationFailedException e)
        {
            return createFailedValidation(context,e);
        }
        catch (Throwable cause)
        {
            return
                createFailedValidation(
                    context,
                    new SemanticValidationFailedException(cause));
        }
    }

    private boolean
    createFailedValidation(C context,SemanticValidationFailedException exception)
    {
        context.completeStepWith(exception);
        return false;
    }
}

//////////////////////////////////////////////////////////////////////////////
