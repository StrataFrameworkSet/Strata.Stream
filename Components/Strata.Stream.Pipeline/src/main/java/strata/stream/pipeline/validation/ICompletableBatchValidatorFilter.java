//////////////////////////////////////////////////////////////////////////////
// ICompletableValidatorFilter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import strata.foundation.core.collection.ICollection;
import strata.stream.core.shared.IFunction;
import strata.stream.pipeline.concurrent.CompletableStreamStage;

import java.io.Serializable;

public
interface ICompletableBatchValidatorFilter<
    T extends Serializable,
    C extends ICollection<T>>
    extends IFunction<CompletableStreamStage<C>,CompletableStreamStage<C>> {}

//////////////////////////////////////////////////////////////////////////////
