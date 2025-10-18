//////////////////////////////////////////////////////////////////////////////
// ICompletableEnricherMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.enrichment;

import strata.stream.core.shared.IFunction;
import strata.stream.pipeline.concurrent.CompletableStreamStage;

import java.io.Serializable;

public
interface ICompletableEnricherMapper<
    I extends Serializable,
    O extends Serializable>
    extends IFunction<CompletableStreamStage<I>,CompletableStreamStage<O>> {}

//////////////////////////////////////////////////////////////////////////////