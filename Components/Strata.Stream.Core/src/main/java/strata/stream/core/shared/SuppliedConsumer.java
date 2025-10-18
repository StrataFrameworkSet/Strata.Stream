/// ///////////////////////////////////////////////////////////////////////////
// SuppliedConsumer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.shared;

public
class SuppliedConsumer<T>
    implements IConsumer<T>
{
    private final ISupplier<IConsumer<T>> supplier;

    public
    SuppliedConsumer(ISupplier<IConsumer<T>> supplier)
    {
        this.supplier = supplier;
    }

    @Override
    public void
    accept(T input)
    {
        supplier
            .get()
            .accept(input);
    }

    public static <T> SuppliedConsumer<T>
    of(ISupplier<IConsumer<T>> supplier)
    {
        return new SuppliedConsumer<>(supplier);
    }

    public static <T> SuppliedConsumer<T>
    of(IConsumer<T> consumer)
    {
        return new SuppliedConsumer<>(() -> consumer);
    }
}

//////////////////////////////////////////////////////////////////////////////
