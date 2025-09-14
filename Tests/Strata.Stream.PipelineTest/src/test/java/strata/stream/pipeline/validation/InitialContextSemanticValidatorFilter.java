//////////////////////////////////////////////////////////////////////////////
// InitialContextSemanticValidatorFilter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import strata.stream.pipeline.context.InitialContext;

public
class InitialContextSemanticValidatorFilter
    extends SemanticValidatorFilter<Long,InitialContext>
    implements IInitialContextSemanticValidatorFilter
{
    public
    InitialContextSemanticValidatorFilter(ISemanticValidator<Long> validator)
    {
        super(validator);
    }
}

//////////////////////////////////////////////////////////////////////////////
