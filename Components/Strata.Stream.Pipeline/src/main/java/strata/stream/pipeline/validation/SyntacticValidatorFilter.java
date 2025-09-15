//////////////////////////////////////////////////////////////////////////////
// SyntacticValidatorFilter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import strata.stream.pipeline.context.IPipelineContext;

public
class SyntacticValidatorFilter<T,C extends IPipelineContext<T>>
    implements ISyntacticValidatorFilter<T,C>
{
    private final ISyntacticValidator<T> validator;

    public
    SyntacticValidatorFilter(ISyntacticValidator<T> validator)
    {
        this.validator = validator;
    }

    @Override
    public boolean
    test(C context)
    {
        context.startStep(".ValidateSyntax");

        try
        {
            doValidate(context);
            return true;
        }
        catch (SyntacticValidationFailedException e)
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
                    new SyntacticValidationFailedException(cause));
        }
    }

    private void
    doValidate(C context) throws SyntacticValidationFailedException
    {
        validator.validate(context.getValue());
        context.completeStep();
    }


    private boolean
    doRecovery(C context,SyntacticValidationFailedException exception)
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
                    new SyntacticValidationFailedException(cause));
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
        catch (SyntacticValidationFailedException e)
        {
            return createFailedValidation(context,e);
        }
        catch (Throwable cause)
        {
            return
                createFailedValidation(
                    context,
                    new SyntacticValidationFailedException(cause));
        }
    }

    private boolean
    createFailedValidation(C context,SyntacticValidationFailedException exception)
    {
        context.completeStepWith(exception);
        return false;
    }
}

//////////////////////////////////////////////////////////////////////////////
