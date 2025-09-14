/// ///////////////////////////////////////////////////////////////////////////
// IToStringContextToUpperContextEnricherMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.enrichment;

import strata.stream.pipeline.context.ToStringContext;
import strata.stream.pipeline.context.ToUpperContext;

public
interface IToStringContextToUpperContextEnricherMapper
    extends IEnricherMapper<String,String,ToStringContext,ToUpperContext>
{}

//////////////////////////////////////////////////////////////////////////////