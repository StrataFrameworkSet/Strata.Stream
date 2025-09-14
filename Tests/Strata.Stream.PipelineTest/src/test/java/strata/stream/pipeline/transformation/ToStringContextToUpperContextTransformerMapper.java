/// ///////////////////////////////////////////////////////////////////////////
// ToStringContextToUpperContextEnricherMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.transformation;

import strata.stream.pipeline.context.ToStringContext;
import strata.stream.pipeline.context.ToUpperContext;
import strata.stream.pipeline.context.ToUpperContextFactory;

public
class ToStringContextToUpperContextTransformerMapper
    extends TransformerMapper<String,String,ToStringContext,ToUpperContext>
    implements IToStringContextToUpperContextTransformerMapper
{
    public
    ToStringContextToUpperContextTransformerMapper(
        ITransformer<String,String> transformer)
    {
        super(transformer,new ToUpperContextFactory());
    }
}

//////////////////////////////////////////////////////////////////////////////
