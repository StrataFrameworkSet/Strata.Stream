/// ///////////////////////////////////////////////////////////////////////////
// InitialContextSyntacticValidatorFilter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import strata.stream.pipeline.context.InitialContext;

public
class InitialContextSyntacticValidatorFilter
    extends SyntacticValidatorFilter<Long,InitialContext>
    implements IInitialContextSyntacticValidatorFilter
{
    public
    InitialContextSyntacticValidatorFilter(ISyntacticValidator<Long> validator)
    {
        super(validator);
    }
}

//////////////////////////////////////////////////////////////////////////////
