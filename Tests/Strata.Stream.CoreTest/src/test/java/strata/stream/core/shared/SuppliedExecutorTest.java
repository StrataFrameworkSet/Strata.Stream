/// ///////////////////////////////////////////////////////////////////////////
// SuppliedExecutorTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.shared;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.Executors;

@Tag("CommitStage")
public
class SuppliedExecutorTest
{
    @Test
    public void
    testSerialization()
    {
        SuppliedExecutor expected =
            SuppliedExecutor.of(() -> Executors.newCachedThreadPool());
        SuppliedExecutor actual = deserialize(serialize(expected));

        actual.execute(() -> System.out.println("Hello, World!"));
    }

    private static byte[]
    serialize(SuppliedExecutor input)
    {
        try (
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);)
        {
            oos.writeObject(input);
            oos.flush();
            return baos.toByteArray();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static SuppliedExecutor
    deserialize(byte[] input)
    {
        try (
            ByteArrayInputStream bais = new ByteArrayInputStream(input);
            ObjectInputStream ois = new ObjectInputStream(bais);)
        {
            return (SuppliedExecutor)ois.readObject();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}

//////////////////////////////////////////////////////////////////////////////
