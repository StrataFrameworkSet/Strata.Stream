//////////////////////////////////////////////////////////////////////////////
// SyntacticValidationException.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.enrichment;

import strata.stream.pipeline.shared.PipelineException;

public
class EnrichmentFailedException
    extends PipelineException
{
    public
    EnrichmentFailedException() { super(); }

    public
    EnrichmentFailedException(String message)
    {
        super(message);
    }

    public
    EnrichmentFailedException(String message,Throwable cause)
    {
        super(message,cause);
    }

    public
    EnrichmentFailedException(Throwable cause)
    {
        super(cause);
    }
}

//////////////////////////////////////////////////////////////////////////////
