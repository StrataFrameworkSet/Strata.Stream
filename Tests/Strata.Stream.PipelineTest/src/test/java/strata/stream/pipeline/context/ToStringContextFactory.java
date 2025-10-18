//////////////////////////////////////////////////////////////////////////////
// ToStringContextFactory.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import strata.foundation.core.utility.ExtendedOptional;
import strata.stream.pipeline.shared.PipelineException;

import java.util.List;
import java.util.Optional;

public
class ToStringContextFactory
    implements IPipelineContextFactory<Long,String,InitialContext,ToStringContext>
{
    @Override
    public ToStringContext
    create(
        InitialContext              input,
        Optional<String>            value,
        Optional<PipelineException> exception)
    {
        List<PipelineStep> accumulated = input.getAccumulatedSteps();

        return
            ExtendedOptional
                .of(exception)
                .ifPresentOrElse(
                    e -> new ToStringContext(e,accumulated),
                    () -> new ToStringContext(value.orElse(""),accumulated));
    }
}

//////////////////////////////////////////////////////////////////////////////
