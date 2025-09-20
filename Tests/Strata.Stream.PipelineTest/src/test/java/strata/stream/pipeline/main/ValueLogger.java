/// ///////////////////////////////////////////////////////////////////////////
// ValueLogger.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.main;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import strata.stream.core.shared.IFunction;

public
class ValueLogger
    implements IFunction<Long,Long>
{
    private final String stage;
    private final Logger logger;

    public
    ValueLogger(String stage)
    {
        this.stage = stage;
        this.logger = LogManager.getLogger(getClass());
    }

    @Override
    public Long
    apply(Long value)
    {
        System.out.println("Value[" + stage + "] = " + value);
        logger.info("Value[{}] = {}", stage, value);
        return value;
    }
}

//////////////////////////////////////////////////////////////////////////////
