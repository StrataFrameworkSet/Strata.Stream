//////////////////////////////////////////////////////////////////////////////
// IToStringContextToUpperContextEnricherMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.transformation;

import strata.stream.pipeline.context.ToStringContext;
import strata.stream.pipeline.context.ToUpperContext;

public
interface IToStringContextToUpperContextTransformerMapper
    extends ITransformerMapper<String,String,ToStringContext,ToUpperContext> {}

//////////////////////////////////////////////////////////////////////////////