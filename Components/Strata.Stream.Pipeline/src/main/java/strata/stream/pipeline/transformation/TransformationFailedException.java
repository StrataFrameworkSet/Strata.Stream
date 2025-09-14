//////////////////////////////////////////////////////////////////////////////
// SyntacticValidationException.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.transformation;

import strata.stream.pipeline.shared.PipelineException;

public
class TransformationFailedException
    extends PipelineException
{
    public
    TransformationFailedException() { super(); }

    public
    TransformationFailedException(String message)
    {
        super(message);
    }

    public
    TransformationFailedException(String message,Throwable cause)
    {
        super(message,cause);
    }

    public
    TransformationFailedException(Throwable cause)
    {
        super(cause);
    }
}

//////////////////////////////////////////////////////////////////////////////
