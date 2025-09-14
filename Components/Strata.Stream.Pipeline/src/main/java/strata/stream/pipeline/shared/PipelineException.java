//////////////////////////////////////////////////////////////////////////////
// PipelineException.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.shared;

public abstract
class PipelineException
    extends RuntimeException
{
    protected
    PipelineException()
    {
        super();
    }

    protected
    PipelineException(String message)
    {
        super(message);
    }

    protected
    PipelineException(Throwable cause)
    {
        super(cause);
    }

    protected
    PipelineException(String message,Throwable cause)
    {
        super(message,cause);
    }

}

//////////////////////////////////////////////////////////////////////////////
