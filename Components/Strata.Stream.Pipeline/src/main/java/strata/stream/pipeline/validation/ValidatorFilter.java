//////////////////////////////////////////////////////////////////////////////
// ValidatorFilter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import strata.stream.pipeline.context.IPipelineContext;

import java.io.Serializable;

public
class ValidatorFilter<
    T extends Serializable,
    C extends IPipelineContext<T>>
    implements IValidatorFilter<T,C>
{
    private final String        step;
    private final IValidator<T> validator;

    public
    ValidatorFilter(String step,IValidator<T> validator)
    {
        this.step = step;
        this.validator = validator;
    }

    @Override
    public boolean
    test(C context)
    {
        context.startStep("." + step);

        try
        {
            ValidationResult<T> result = validator.validate(context.getValue());

            if (result.isValid())
            {
                context.completeStep();
                return true;
            }
            else
            {
                ValidationFailedException exception = ValidationFailedException.of(result);

                if (context.isRecoverable(exception))
                    return doRecovery(context,exception);

                return createFailedValidation(context,exception);
            }
        }
        catch (Throwable cause)
        {
            return
                createFailedValidation(
                    context,
                    new ValidationFailedException(cause));
        }
    }

    private boolean
    doRecovery(C context,ValidationFailedException exception)
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
                    new ValidationFailedException(cause));
        }
    }

    private boolean
    reTest(C context)
    {
        try
        {
            ValidationResult<T> result = validator.validate(context.getValue());

            if (result.isValid())
            {
                context.completeStep();
                return true;
            }
            else
                return
                    createFailedValidation(
                        context,
                        ValidationFailedException.of(result));
        }
        catch (Throwable cause)
        {
            return
                createFailedValidation(
                    context,
                    new ValidationFailedException(cause));
        }
    }

    private boolean
    createFailedValidation(C context,ValidationFailedException exception)
    {
        context.completeStepWith(exception);
        return false;
    }
}

//////////////////////////////////////////////////////////////////////////////
