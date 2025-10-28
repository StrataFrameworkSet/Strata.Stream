/// ///////////////////////////////////////////////////////////////////////////
// CompletableWindowLogger.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.shared;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import strata.foundation.core.collection.ICollection;
import strata.stream.core.shared.IFunction;
import strata.stream.pipeline.concurrent.CompletableStreamStage;
import strata.stream.pipeline.concurrent.ICompletableContext;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public
class CompletableWindowLogger<T extends Serializable>
    implements
        IFunction<
            CompletableStreamStage<ICollection<T>>,
            CompletableStreamStage<ICollection<T>>>
{
    private final static AtomicInteger counter = new AtomicInteger(0);

    @Override
    public CompletableStreamStage<ICollection<T>>
    apply(CompletableStreamStage<ICollection<T>> stage)
    {
        return stage.thenApply(this::logWindow);
    }

    public static void
    initialize() { counter.set(0); }

    private ICompletableContext<ICollection<T>>
    logWindow(ICompletableContext<ICollection<T>> context)
    {
        if (!context.isFilteredOut())
        {
            try
            {
                int windowCount = counter.incrementAndGet();

                getLogger()
                    .info(
                        "\n\n" +
                        "########################################\n" +
                        "#\n" +
                        "#  Window {}: size = {}\n" +
                        "#\n" +
                        "########################################\n",
                        windowCount,
                        context.getValue().size());
            }
            catch (Throwable e)
            {
                getLogger().error(e);
            }
        }

        return context;
    }

    private Logger
    getLogger() { return LogManager.getLogger(getClass()); }
}

//////////////////////////////////////////////////////////////////////////////
