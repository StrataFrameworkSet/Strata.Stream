/// ///////////////////////////////////////////////////////////////////////////
// IPipelineContextFactory.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import strata.stream.pipeline.shared.PipelineException;

import java.io.Serializable;
import java.util.Optional;

public
interface IPipelineContextFactory<
    I extends Serializable,
    O extends Serializable,
    CI extends IPipelineContext<I>,
    CO extends IPipelineContext<O>>
    extends Serializable
{
    CO
    create(CI input,Optional<O> value,Optional<PipelineException> exception);

    default CO
    create(CI input,O value)
    {
        return
            create(
                input,
                Optional.ofNullable(value),
                Optional.empty());
    }

    ;

    default CO
    create(CI input,PipelineException exception)
    {
        return
            create(
                input,
                Optional.empty(),
                Optional.ofNullable(exception));
    }

    ;
}

//////////////////////////////////////////////////////////////////////////////