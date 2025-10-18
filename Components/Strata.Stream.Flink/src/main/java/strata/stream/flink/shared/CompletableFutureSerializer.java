//////////////////////////////////////////////////////////////////////////////
// OptionalSerializer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.concurrent.CompletableFuture;

public
class CompletableFutureSerializer
    extends Serializer<CompletableFuture<?>>
{
    @Override
    public void
    write(Kryo kryo,Output output,CompletableFuture<?> value)
    {
        kryo.writeClassAndObject(output,value.join());
    }

    @Override
    public CompletableFuture<?>
    read(Kryo kryo,Input input,Class<CompletableFuture<?>> type)
    {
        return
            CompletableFuture.completedFuture(
                kryo.readClassAndObject(input));
    }
}

//////////////////////////////////////////////////////////////////////////////
