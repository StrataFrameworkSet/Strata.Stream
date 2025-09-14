/// ///////////////////////////////////////////////////////////////////////////
// IInitialContextToStringContextEnricherMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.transformation;

import strata.stream.pipeline.context.InitialContext;
import strata.stream.pipeline.context.ToStringContext;

public
interface IInitialContextToStringContextTransformerMapper
    extends ITransformerMapper<Long,String,InitialContext,ToStringContext> {}

//////////////////////////////////////////////////////////////////////////////