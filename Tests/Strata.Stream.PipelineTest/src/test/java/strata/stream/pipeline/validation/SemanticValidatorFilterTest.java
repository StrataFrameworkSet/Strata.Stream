//////////////////////////////////////////////////////////////////////////////
// SemanticValidatorFilterTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import strata.stream.pipeline.context.InitialContext;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("CommitStage")
public
class SemanticValidatorFilterTest
{
    private IInitialContextSemanticValidatorFilter filter;

    @BeforeEach
    public void
    setUp()
    {
        filter =
            new InitialContextSemanticValidatorFilter(
                (input) -> isPositive(input));

    }

    @ParameterizedTest
    @MethodSource("provideInitialContexts")
    public void
    testTest(InitialContext context,boolean expected)
        throws Exception
    {
        boolean actual = filter.test(context);

        assertEquals(expected,actual);
    }

    private static Stream<Arguments>
    provideInitialContexts()
    {
        return
            Stream.of(
                Arguments.of(InitialContext.of(-5L),false),
                Arguments.of(InitialContext.of(-4L),false),
                Arguments.of(InitialContext.of(-3L),false),
                Arguments.of(InitialContext.of(-2L),false),
                Arguments.of(InitialContext.of(-1L),false),
                Arguments.of(InitialContext.of(0L),false),
                Arguments.of(InitialContext.of(1L),true),
                Arguments.of(InitialContext.of(2L),true),
                Arguments.of(InitialContext.of(3L),true),
                Arguments.of(InitialContext.of(4L),true),
                Arguments.of(InitialContext.of(5L),true));
    }

    private static void
    isPositive(Long input)
    {
        if (input > 0)
            return;

        throw new SemanticValidationFailedException("Not positive");
    }
}

//////////////////////////////////////////////////////////////////////////////
