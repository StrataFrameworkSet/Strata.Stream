/// ///////////////////////////////////////////////////////////////////////////
// InitialContextSyntacticValidatorFilter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import strata.stream.pipeline.context.InitialContext;

public
class InitialContextValidatorFilter
    extends ValidatorFilter<Long,InitialContext>
    implements IInitialContextValidatorFilter
{
    public
    InitialContextValidatorFilter(String step,IValidator<Long> validator)
    {
        super(step,validator);
    }
}

//////////////////////////////////////////////////////////////////////////////
