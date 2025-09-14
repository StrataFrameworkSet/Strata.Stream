//////////////////////////////////////////////////////////////////////////////
// SyntacticValidationFailedException.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import java.util.Set;

public
class SyntacticValidationFailedException
    extends ValidationException
{
    public
    SyntacticValidationFailedException() { super(); }

    public
    SyntacticValidationFailedException(Set<Integer> failureCodes)
    {
        super(failureCodes);
    }

    public
    SyntacticValidationFailedException(String message)
    {
        super(message);
    }

    public
    SyntacticValidationFailedException(
        Set<Integer> failureCodes,
        String       message)
    {
        super(failureCodes,message);
    }

    public
    SyntacticValidationFailedException(String message,Throwable cause)
    {
        super(message,cause);
    }

    public
    SyntacticValidationFailedException(
        Set<Integer> failureCodes,
        String       message,
        Throwable    cause)
    {
        super(failureCodes,message,cause);
    }

    public
    SyntacticValidationFailedException(Throwable cause)
    {
        super(cause);
    }


    public
    SyntacticValidationFailedException(
        Set<Integer> failureCodes,
        Throwable    cause)
    {
        super(failureCodes,cause);
    }

}

//////////////////////////////////////////////////////////////////////////////
