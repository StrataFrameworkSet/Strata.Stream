/// ///////////////////////////////////////////////////////////////////////////
// SyntacticValidatorFilterTest.java
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
class SyntacticValidatorFilterTest
{
    private IInitialContextSyntacticValidatorFilter filterA;
    private IInitialContextSyntacticValidatorFilter filterB;


    @BeforeEach
    public void
    setUp()
    {
        filterA =
            new InitialContextSyntacticValidatorFilter(
                (input) -> isEven(input));
        filterB =
            new InitialContextSyntacticValidatorFilter(
                (input) -> isOdd(input));
    }

    @ParameterizedTest
    @MethodSource("provideInitialContexts")
    public void
    testTest(
        InitialContext contextA,
        InitialContext contextB,
        boolean expectedA,
        boolean expectedB)
        throws Exception
    {
        boolean actualA = filterA.test(contextA);
        boolean actualB = filterB.test(contextB);

        assertEquals(actualA,expectedA);
        assertEquals(actualB,expectedB);
    }

    private static Stream<Arguments>
    provideInitialContexts()
    {
        return
            Stream.of(
                Arguments.of(InitialContext.of(1L),InitialContext.of(1L),false,true),
                Arguments.of(InitialContext.of(2L),InitialContext.of(2L),true,false),
                Arguments.of(InitialContext.of(3L),InitialContext.of(3L),false,true),
                Arguments.of(InitialContext.of(4L),InitialContext.of(4L),true,false),
                Arguments.of(InitialContext.of(5L),InitialContext.of(5L),false,true));
    }

    private static void
    isEven(Long input)
    {
        if (input % 2 == 0)
            return;

        throw new SyntacticValidationFailedException("Not even");
    }

    private static void
    isOdd(Long input)
    {
        if (input % 2 != 0)
            return;

        throw new SyntacticValidationFailedException("Not odd");
    }
}

//////////////////////////////////////////////////////////////////////////////
