/// ///////////////////////////////////////////////////////////////////////////
// InitialContextToStringContextEnricherMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.enrichment;

import strata.stream.pipeline.context.InitialContext;
import strata.stream.pipeline.context.ToStringContext;
import strata.stream.pipeline.context.ToStringContextFactory;
import strata.stream.pipeline.transformation.ITransformer;
import strata.stream.pipeline.transformation.TransformerMapper;

public
class InitialContextToStringContextEnricherMapper
    extends TransformerMapper<Long,String,InitialContext,ToStringContext>
    implements IInitialContextToStringContextEnricherMapper
{
    public InitialContextToStringContextEnricherMapper(ITransformer<Long,String> transformer)
    {
        super(transformer,new ToStringContextFactory());
    }
}

//////////////////////////////////////////////////////////////////////////////
