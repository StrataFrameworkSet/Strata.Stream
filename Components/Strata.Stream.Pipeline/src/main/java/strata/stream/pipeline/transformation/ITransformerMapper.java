//////////////////////////////////////////////////////////////////////////////
// ITransformerMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.transformation;

import strata.stream.core.shared.IFunction;
import strata.stream.pipeline.context.IPipelineContext;

import java.io.Serializable;

public
interface ITransformerMapper<
    I extends Serializable,
    O extends Serializable,
    CI extends IPipelineContext<I>,
    CO extends IPipelineContext<O>>
    extends IFunction<CI,CO>
{}

//////////////////////////////////////////////////////////////////////////////