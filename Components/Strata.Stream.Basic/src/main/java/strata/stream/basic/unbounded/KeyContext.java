/// ///////////////////////////////////////////////////////////////////////////
// KeyContext.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.unbounded;

public
class KeyContext<K,T>
{
    public final K key;
    public final T value;

    public
    KeyContext(K key, T value)
    {
        this.key = key;
        this.value = value;
    }

    public K
    getKey()
    {
        return key;
    }

    public T
    getValue()
    {
        return value;
    }

    public static <K,T> KeyContext<K,T>
    of(K key, T value)
    {
        return new KeyContext<>(key,value);
    }
}

//////////////////////////////////////////////////////////////////////////////
