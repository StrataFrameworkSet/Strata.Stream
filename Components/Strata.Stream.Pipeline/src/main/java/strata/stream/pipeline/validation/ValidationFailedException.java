//////////////////////////////////////////////////////////////////////////////
// SemanticValidationFailedException.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import java.util.Optional;

public
class ValidationFailedException
    extends ValidationException
{
    private final ValidationResult<?> result;

    public
    ValidationFailedException()
    {
        super();
        this.result = null;
    }

    public
    ValidationFailedException(ValidationResult<?> result)
    {
        super(result.getCodes());
        this.result = result;
    }

    public
    ValidationFailedException(String message)
    {
        super(message);
        this.result = null;
    }

    public
    ValidationFailedException(
        ValidationResult<?> result,
        String              message)
    {
        super(result.getCodes(),message);
        this.result = result;
    }

    public
    ValidationFailedException(String message,Throwable cause)
    {
        super(message,cause);
        this.result = null;
    }

    public
    ValidationFailedException(
        ValidationResult<?> result,
        String              message,
        Throwable           cause)
    {
        super(result.getCodes(),message,cause);
        this.result = result;
    }

    public
    ValidationFailedException(Throwable cause)
    {
        super(cause);
        this.result = null;
    }

    public
    ValidationFailedException(
        ValidationResult<?> result,
        Throwable           cause)
    {
        super(result.getCodes(),cause);
        this.result = result;
    }

    public Optional<ValidationResult<?>>
    getResult()
    {
        return Optional.ofNullable(result);
    }

    public static ValidationFailedException
    of(ValidationResult<?> result)
    {
        return new ValidationFailedException(result);
    }
}

//////////////////////////////////////////////////////////////////////////////
