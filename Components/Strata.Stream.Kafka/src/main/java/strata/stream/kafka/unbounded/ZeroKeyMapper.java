//////////////////////////////////////////////////////////////////////////////
// ZeroKeyMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import strata.foundation.core.reflect.TypeLiteral;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public
class ZeroKeyMapper
{
    private final Map<Type,Object>     mappings;
    private static final ZeroKeyMapper instance = new ZeroKeyMapper();

    private
    ZeroKeyMapper()
    {
        mappings = new ConcurrentHashMap<>();
        addMapping(Boolean.class,false);
        addMapping(Byte.class,(byte)0);
        addMapping(Short.class,(short)0);
        addMapping(Integer.class,0);
        addMapping(Long.class,0L);
        addMapping(Float.class,0.0f);
        addMapping(Double.class,0.0);
        addMapping(Character.class,'\0');
        addMapping(String.class,"");
    }

    public <T> ZeroKeyMapper
    addMapping(Class<T> type,T zero)
    {
        mappings.put(type,zero);
        return this;
    }

    public <T> ZeroKeyMapper
    addMapping(TypeLiteral<T> type,T zero)
    {
        mappings.put(type.getType(),zero);
        return this;
    }

    public <T> boolean
    hasMapping(Class<T> type)
    {
        return mappings.containsKey(type);
    }

    public <T> boolean
    hasMapping(TypeLiteral<T> type)
    {
        return mappings.containsKey(type.getType());
    }

    public <T> Optional<T>
    map(Class<T> type)
    {
        return
            Optional.ofNullable(type.cast(mappings.get(type)));
    }

    public <T> Optional<T>
    map(TypeLiteral<T> type)
    {
        return
            Optional.ofNullable(
                type
                    .getRawType()
                    .cast(mappings.get(type.getType())));
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<T>
    map(T instance)
    {
        if (instance == null)
            return Optional.empty();

        return Optional.ofNullable((T)mappings.get(instance.getClass()));
    }

    public static ZeroKeyMapper
    getInstance() { return instance; }
}

//////////////////////////////////////////////////////////////////////////////
