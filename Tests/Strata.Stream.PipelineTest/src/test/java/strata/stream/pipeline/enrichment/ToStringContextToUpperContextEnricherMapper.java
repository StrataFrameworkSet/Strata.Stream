//////////////////////////////////////////////////////////////////////////////
// ToStringContextToUpperContextEnricherMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.enrichment;

import strata.stream.pipeline.context.ToStringContext;
import strata.stream.pipeline.context.ToUpperContext;
import strata.stream.pipeline.context.ToUpperContextFactory;
import strata.stream.pipeline.transformation.ITransformer;
import strata.stream.pipeline.transformation.TransformerMapper;

public
class ToStringContextToUpperContextEnricherMapper
    extends TransformerMapper<String,String,ToStringContext,ToUpperContext>
    implements IToStringContextToUpperContextEnricherMapper
{
    public ToStringContextToUpperContextEnricherMapper(
        ITransformer<String,String> transformer)
    {
        super(transformer,new ToUpperContextFactory());
    }
}

//////////////////////////////////////////////////////////////////////////////
