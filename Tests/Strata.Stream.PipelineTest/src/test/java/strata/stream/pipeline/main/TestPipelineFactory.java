/// ///////////////////////////////////////////////////////////////////////////
// TestPipelineFactory.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.main;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import strata.stream.core.unbounded.IUnboundedStreamExecutor;
import strata.stream.core.unbounded.IUnboundedStreamSource;
import strata.stream.pipeline.context.InitialContext;
import strata.stream.pipeline.context.ToStringContext;
import strata.stream.pipeline.context.ToUpperContext;
import strata.stream.pipeline.enrichment.IInitialContextToStringContextEnricherMapper;
import strata.stream.pipeline.transformation.IToStringContextToUpperContextTransformerMapper;
import strata.stream.pipeline.validation.*;

public
class TestPipelineFactory
    extends AbstractPipelineFactory<Long,IUnboundedStreamSource<Long>>
    implements ITestPipelineFactory
{
    private final IInitialContextSyntacticValidatorFilter         syntactic;
    private final IInitialContextSemanticValidatorFilter          semantic;
    private final IInitialContextToStringContextEnricherMapper    enricher;
    private final IToStringContextToUpperContextTransformerMapper transformer;

    public
    TestPipelineFactory(
        IInitialContextSyntacticValidatorFilter         syntactic,
        IInitialContextSemanticValidatorFilter          semantic,
        IInitialContextToStringContextEnricherMapper    enricher,
        IToStringContextToUpperContextTransformerMapper transformer)
    {
        super();
        this.syntactic = syntactic;
        this.semantic = semantic;
        this.enricher = enricher;
        this.transformer = transformer;
    }

    @Override
    protected IUnboundedStreamSource<Long>
    getSource(Class<Long> inputType)
    {
        return
            new FromDataUnboundedStreamSource<>(
                inputType,
                -5L,-4L,-3L,-2L,-1L,0L,1L,2L,3L,4L,5L,11L,12L,13L,14L,15L);
    }

    @Override
    protected IUnboundedStreamExecutor
    configure(IUnboundedStreamSource<Long> source)
    {
        return
            source
                .map(new ValueLogger("Source"))
                .map(input -> InitialContext.of(input))
                .map(context -> logContext(context,"ContextCreation"))
                .filter(syntactic)
                .map(context -> logContext(context,"SyntacticValidation"))
                .filter(semantic)
                .filter(
                    new InitialContextValidatorFilter(
                        "CheckLimit",
                        new OptionalLimitValidator(10L)))
                .filter(
                    new InitialContextValidatorFilter(
                        "CheckNoLimit",
                        new OptionalLimitValidator()))
                .map(context -> logContext(context,"SemanticValidation"))
                .map(enricher)
                .map(context -> logContext(context,"Enrichment"))
                .map(transformer)
                .map(context -> logContext(context,"Transformation"))
                .map(ToUpperContext::getValue)
                .forEach(value -> logValue(value,"FinalValue"));
    }

    private Logger
    getLogger()
    {
        return LogManager.getLogger(getClass());
    }

    private Long
    logValue(Long value,String stage)
    {
        getLogger().info("Value[{}]: {}",stage,value);
        return value;
    }

    private String
    logValue(String value,String stage)
    {
        getLogger().info("Value[{}]: {}",stage,value);
        return value;
    }

    private InitialContext
    logContext(InitialContext context,String stage)
    {
        getLogger().info("InitialContext[{}]: {}",stage,context.getValue());
        return context;
    }


    private ToStringContext
    logContext(ToStringContext context,String stage)
    {
        getLogger().info("ToStringContext[{}]: {}",stage,context.getValue());
        return context;
    }


    private ToUpperContext
    logContext(ToUpperContext context,String stage)
    {
        getLogger().info("ToUpperContext[{}]: {}",stage,context.getValue());
        return context;
    }

}

//////////////////////////////////////////////////////////////////////////////
