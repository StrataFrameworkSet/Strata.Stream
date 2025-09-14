/// ///////////////////////////////////////////////////////////////////////////
// IPipelineContextFactory.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import strata.stream.pipeline.shared.PipelineException;

import java.util.Optional;

public
interface IPipelineContextFactory<
    I,
    O,
    CI extends IPipelineContext<I>,
    CO extends IPipelineContext<O>>
{
    CO
    create(CI input,Optional<O> value,Optional<PipelineException> exception);

    default
    CO
    create(CI input,O value)
    {
        return
            create(
                input,
                Optional.ofNullable(value),
                Optional.empty());
    };

    default
    CO
    create(CI input,PipelineException exception)
    {
        return
            create(
                input,
                Optional.empty(),
                Optional.ofNullable(exception));
    };
}

//////////////////////////////////////////////////////////////////////////////