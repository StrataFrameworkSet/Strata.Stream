//////////////////////////////////////////////////////////////////////////////
// ICompletableValidator.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import strata.foundation.core.collection.ICollection;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

public
interface ICompletableBatchValidator<
    T extends Serializable,
    C extends ICollection<T>>
    extends Serializable
{
    CompletionStage<ICollection<ValidationResult<T>>>
    validate(C value);
}

//////////////////////////////////////////////////////////////////////////////