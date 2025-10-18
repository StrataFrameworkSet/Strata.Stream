//////////////////////////////////////////////////////////////////////////////
// ICompletableValidatorFilter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import strata.stream.core.shared.IFunction;
import strata.stream.pipeline.concurrent.CompletableStreamStage;

import java.io.Serializable;

public
interface ICompletableValidatorFilter<T extends Serializable>
    extends IFunction<CompletableStreamStage<T>,CompletableStreamStage<T>> {}

//////////////////////////////////////////////////////////////////////////////
