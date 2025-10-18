/// ///////////////////////////////////////////////////////////////////////////
// SuppliedKeySelector.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.shared;

public
class SuppliedKeySelector<K,T>
    implements IKeySelector<K,T>
{
    private final ISupplier<IKeySelector<K,T>> supplier;

    public
    SuppliedKeySelector(ISupplier<IKeySelector<K,T>> supplier)
    {
        this.supplier = supplier;
    }

    @Override
    public K
    getKey(T item)
    {
        return
            supplier
                .get()
                .getKey(item);
    }

    public static <K,T> SuppliedKeySelector<K,T>
    of(ISupplier<IKeySelector<K,T>> supplier)
    {
        return new SuppliedKeySelector<>(supplier);
    }

    public static <K,T> SuppliedKeySelector<K,T>
    of(IKeySelector<K,T> keySelector)
    {
        return new SuppliedKeySelector<>(() -> keySelector);
    }
}


//////////////////////////////////////////////////////////////////////////////
