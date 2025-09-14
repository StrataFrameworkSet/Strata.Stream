//////////////////////////////////////////////////////////////////////////////
// SyntacticValidationException.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import strata.stream.pipeline.shared.PipelineException;

import java.util.Set;

public abstract
class ValidationException
    extends PipelineException
{
    private final Set<Integer> failureCodes;

    protected
    ValidationException()
    {
        this(Set.of());
    }

    protected
    ValidationException(Set<Integer> failureCodes)
    {
        super();
        this.failureCodes = Set.copyOf(failureCodes);
    }

    protected
    ValidationException(String message)
    {
        this(Set.of(),message);
    }

    protected
    ValidationException(Set<Integer> failureCodes,String message)
    {
        super(message);
        this.failureCodes = Set.copyOf(failureCodes);
    }

    protected
    ValidationException(String message,Throwable cause)
    {
        this(Set.of(),message,cause);
    }

    protected
    ValidationException(
        Set<Integer> failureCodes,
        String       message,
        Throwable    cause)
    {
        super(message,cause);
        this.failureCodes = Set.copyOf(failureCodes);
    }

    protected
    ValidationException(Throwable cause)
    {
        this(Set.of(),cause);
    }

    protected
    ValidationException(Set<Integer> failureCodes,Throwable cause)
    {
        super(cause);
        this.failureCodes = Set.copyOf(failureCodes);
    }

    public Set<Integer>
    getFailureCodes() { return failureCodes; }
}

//////////////////////////////////////////////////////////////////////////////
