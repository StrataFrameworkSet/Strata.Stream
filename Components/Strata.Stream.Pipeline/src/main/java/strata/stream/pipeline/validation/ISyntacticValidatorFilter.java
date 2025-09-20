/// ///////////////////////////////////////////////////////////////////////////
// ISyntacticValidatorFilter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import strata.stream.core.shared.IPredicate;
import strata.stream.pipeline.context.IPipelineContext;

import java.io.Serializable;

public
interface ISyntacticValidatorFilter<
    T extends Serializable,
    C extends IPipelineContext<T>>
    extends IPredicate<C>
{}

//////////////////////////////////////////////////////////////////////////////