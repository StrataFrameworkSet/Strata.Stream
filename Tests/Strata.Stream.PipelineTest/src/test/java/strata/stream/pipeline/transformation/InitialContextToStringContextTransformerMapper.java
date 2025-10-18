//////////////////////////////////////////////////////////////////////////////
// InitialContextToStringContextEnricherMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.transformation;

import strata.stream.pipeline.context.InitialContext;
import strata.stream.pipeline.context.ToStringContext;
import strata.stream.pipeline.context.ToStringContextFactory;

public
class InitialContextToStringContextTransformerMapper
    extends TransformerMapper<Long,String,InitialContext,ToStringContext>
    implements IInitialContextToStringContextTransformerMapper
{
    public
    InitialContextToStringContextTransformerMapper(ITransformer<Long,String> transformer)
    {
        super(transformer,new ToStringContextFactory());
    }
}

//////////////////////////////////////////////////////////////////////////////
