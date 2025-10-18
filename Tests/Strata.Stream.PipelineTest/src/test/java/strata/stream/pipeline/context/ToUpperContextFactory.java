//////////////////////////////////////////////////////////////////////////////
// ToStringContextFactory.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import strata.foundation.core.utility.ExtendedOptional;
import strata.stream.pipeline.shared.PipelineException;

import java.util.List;
import java.util.Optional;

public
class ToUpperContextFactory
    implements IPipelineContextFactory<String,String,ToStringContext,ToUpperContext>
{
    @Override
    public ToUpperContext
    create(
        ToStringContext input,
        Optional<String> value,
        Optional<PipelineException> exception)
    {
        List<PipelineStep> accumulated = input.getAccumulatedSteps();

        return
            ExtendedOptional
                .of(exception)
                .ifPresentOrElse(
                    e -> new ToUpperContext(e,accumulated),
                    () -> new ToUpperContext(value.orElse(""),accumulated));
    }
}

//////////////////////////////////////////////////////////////////////////////
