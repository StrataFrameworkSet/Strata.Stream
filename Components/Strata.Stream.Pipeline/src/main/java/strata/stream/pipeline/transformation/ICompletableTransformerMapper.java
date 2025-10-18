//////////////////////////////////////////////////////////////////////////////
// ICompletableTransformerMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.transformation;

import strata.stream.core.shared.IFunction;
import strata.stream.pipeline.concurrent.CompletableStreamStage;

import java.io.Serializable;

public
interface ICompletableTransformerMapper<
    I extends Serializable,
    O extends Serializable>
    extends IFunction<CompletableStreamStage<I>,CompletableStreamStage<O>> {}

//////////////////////////////////////////////////////////////////////////////