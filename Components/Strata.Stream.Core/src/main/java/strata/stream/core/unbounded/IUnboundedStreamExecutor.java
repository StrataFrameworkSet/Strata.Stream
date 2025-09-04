//////////////////////////////////////////////////////////////////////////////
// IUnboundedStreamExecutor.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.unbounded;

import strata.stream.core.shared.IExtension;
import strata.stream.core.shared.IStreamExecution;

import java.util.Properties;
import java.util.concurrent.CompletionStage;

public
interface IUnboundedStreamExecutor
    extends IExtension
{
    CompletionStage<IStreamExecution>
    execute() throws Exception;

    CompletionStage<IStreamExecution>
    execute(IExecutionDriver driver) throws Exception;

    CompletionStage<IStreamExecution>
    execute(Properties properties) throws Exception;
}

//////////////////////////////////////////////////////////////////////////////