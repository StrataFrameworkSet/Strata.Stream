/// ///////////////////////////////////////////////////////////////////////////
// SuppliedPredicate.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.shared;

public
class SuppliedPredicate<T>
    implements IPredicate<T>
{
    private final ISupplier<IPredicate<T>> supplier;

    public
    SuppliedPredicate(ISupplier<IPredicate<T>> supplier)
    {
        this.supplier = supplier;
    }

    @Override
    public boolean
    test(T item)
    {
        return
            supplier
                .get()
                .test(item);
    }

    public static <T> SuppliedPredicate<T>
    of(ISupplier<IPredicate<T>> supplier)
    {
        return new SuppliedPredicate<>(supplier);
    }

    public static <T> SuppliedPredicate<T>
    of(IPredicate<T> predicate)
    {
        return new SuppliedPredicate<>(() -> predicate);
    }
}

//////////////////////////////////////////////////////////////////////////////
