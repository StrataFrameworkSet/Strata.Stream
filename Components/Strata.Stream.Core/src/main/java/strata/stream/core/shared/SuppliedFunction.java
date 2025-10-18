/// ///////////////////////////////////////////////////////////////////////////
// SuppliedFunction.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.shared;

public
class SuppliedFunction<I,O>
    implements IFunction<I,O>
{
    private final ISupplier<IFunction<I,O>> supplier;

    public
    SuppliedFunction(ISupplier<IFunction<I,O>> supplier)
    {
        this.supplier = supplier;
    }

    @Override
    public O
    apply(I input)
    {
        return
            supplier
                .get()
                .apply(input);
    }

    public static <I,O> SuppliedFunction<I,O>
    of(ISupplier<IFunction<I,O>> supplier)
    {
        return new SuppliedFunction<>(supplier);
    }

    public static <I,O> SuppliedFunction<I,O>
    of(IFunction<I,O> function)
    {
        return new SuppliedFunction<>(() -> function);
    }
}

//////////////////////////////////////////////////////////////////////////////
