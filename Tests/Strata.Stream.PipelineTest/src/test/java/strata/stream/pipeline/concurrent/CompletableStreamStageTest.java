/// ///////////////////////////////////////////////////////////////////////////
// CompletableStreamStageTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.concurrent;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import strata.stream.core.shared.SuppliedExecutor;
import strata.stream.pipeline.context.PipelineContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("CommitStage")
public
class CompletableStreamStageTest
{
    @Test
    public void
    testSerialization()
    {
        CompletableStreamStage<String> expected = 
            CompletableStreamStage.of(
                () -> PipelineContext.of("test"),
                SuppliedExecutor.of(() -> Executors.newCachedThreadPool()));
        CompletableStreamStage<String> actual =
            deserialize(serialize(expected));

        assertEquals(expected.getKey(), actual.getKey());
        assertEquals(expected.getExecutor().getClass(), actual.getExecutor().getClass());
    }

    private static byte[]
    serialize(CompletableStreamStage<String> input)
    {
        try(
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos))
        {
            oos.writeObject(input);
            return baos.toByteArray();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static CompletableStreamStage<String>
    deserialize(byte[] input)
    {
        try (
            ByteArrayInputStream bais =
                new ByteArrayInputStream(input);
            ObjectInputStream ois =
                new ObjectInputStream(bais))
        {
            return (CompletableStreamStage<String>)ois.readObject();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}

//////////////////////////////////////////////////////////////////////////////
