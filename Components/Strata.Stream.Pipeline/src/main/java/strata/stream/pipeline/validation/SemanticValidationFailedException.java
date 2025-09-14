//////////////////////////////////////////////////////////////////////////////
// SemanticValidationFailedException.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import java.util.Set;

public
class SemanticValidationFailedException
    extends ValidationException
{
    public
    SemanticValidationFailedException() { super(); }

    public
    SemanticValidationFailedException(Set<Integer> failureCodes)
    {
        super(failureCodes);
    }

    public
    SemanticValidationFailedException(String message)
    {
        super(message);
    }

    public
    SemanticValidationFailedException(
        Set<Integer> failureCodes,
        String       message)
    {
        super(failureCodes,message);
    }

    public
    SemanticValidationFailedException(String message,Throwable cause)
    {
        super(message,cause);
    }

    public
    SemanticValidationFailedException(
        Set<Integer> failureCodes,
        String       message,
        Throwable    cause)
    {
        super(failureCodes,message,cause);
    }

    public
    SemanticValidationFailedException(Throwable cause)
    {
        super(cause);
    }

    public
    SemanticValidationFailedException(
        Set<Integer> failureCodes,
        Throwable    cause)
    {
        super(failureCodes,cause);
    }

}

//////////////////////////////////////////////////////////////////////////////
