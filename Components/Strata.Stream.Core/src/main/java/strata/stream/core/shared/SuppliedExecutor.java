/// ///////////////////////////////////////////////////////////////////////////
// SuppliedExecutor.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.shared;

import strata.foundation.core.utility.ExtendedOptional;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.*;

public
class SuppliedExecutor
    implements IExecutor
{
    private ISupplier<Executor>        supplier;
    private ExtendedOptional<Executor> executor;

    public
    SuppliedExecutor(ISupplier<Executor> supplier)
    {
        this.supplier = supplier;
        this.executor = ExtendedOptional.empty();
    }

    @Override
    public void
    execute(Runnable command)
    {
        getExecutor().execute(command);
    }

    public static SuppliedExecutor
    of(ISupplier<Executor> supplier)
    {
        return new SuppliedExecutor(supplier);
    }

    private Executor
    getExecutor()
    {
        executor.ifEmpty(
            () -> executor = ExtendedOptional.of(supplier.get()));

        return executor.orElseThrow();
    }

    private void
    writeObject(ObjectOutputStream out) throws IOException
    {
        out.writeObject(supplier);
    }

    @SuppressWarnings("unchecked")
    private void
    readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        supplier = (ISupplier<Executor>)in.readObject();
        executor = ExtendedOptional.empty();
    }
}

//////////////////////////////////////////////////////////////////////////////
