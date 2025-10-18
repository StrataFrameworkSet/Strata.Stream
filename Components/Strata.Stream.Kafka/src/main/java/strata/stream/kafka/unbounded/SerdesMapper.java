//////////////////////////////////////////////////////////////////////////////
// SerdesMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import strata.foundation.core.reflect.TypeLiteral;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public
class SerdesMapper
{
    private final Map<Type,Serde<?>>  mappings;
    private static final SerdesMapper instance = new SerdesMapper();

    private
    SerdesMapper()
    {
        this.mappings = new ConcurrentHashMap<>();
        addMapping(Boolean.class,Serdes.Boolean());
        addMapping(String.class,Serdes.String());
        addMapping(Long.class,Serdes.Long());
        addMapping(Integer.class,Serdes.Integer());
        addMapping(Double.class,Serdes.Double());
        addMapping(Float.class,Serdes.Float());
        addMapping(Short.class,Serdes.Short());
        addMapping(byte[].class,Serdes.ByteArray());
        addMapping(
            new TypeLiteral<List<Boolean>>() {},
            Serdes.ListSerde(ArrayList.class,Serdes.Boolean()));
        addMapping(
            new TypeLiteral<List<String>>() {},
            Serdes.ListSerde(ArrayList.class,Serdes.String()));
        addMapping(
            new TypeLiteral<List<Long>>() {},
            Serdes.ListSerde(ArrayList.class,Serdes.Long()));
        addMapping(
            new TypeLiteral<List<Integer>>() {},
            Serdes.ListSerde(ArrayList.class,Serdes.Integer()));
        addMapping(
            new TypeLiteral<List<Double>>() {},
            Serdes.ListSerde(ArrayList.class,Serdes.Double()));
        addMapping(
            new TypeLiteral<List<Float>>() {},
            Serdes.ListSerde(ArrayList.class,Serdes.Float()));
        addMapping(
            new TypeLiteral<List<Short>>() {},
            Serdes.ListSerde(ArrayList.class,Serdes.Short()));
    }

    public <T> SerdesMapper
    addMapping(Class<T> type,Serde<T> serde)
    {
        mappings.put(type,serde);
        return this;
    }

    public <T> SerdesMapper
    addMapping(TypeLiteral<T> type,Serde<T> serde)
    {
        mappings.put(type.getType(),serde);
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

    @SuppressWarnings("unchecked")
    public <T> Optional<Serde<T>>
    map(Class<T> type)
    {
        return Optional.ofNullable((Serde<T>)mappings.get(type));
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<Serde<T>>
    map(TypeLiteral<T> type)
    {
        return Optional.ofNullable((Serde<T>)mappings.get(type.getType()));
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<Serde<T>>
    map(T instance)
    {
        if (instance == null)
            return Optional.empty();

        return Optional.ofNullable((Serde<T>)mappings.get(instance.getClass()));
    }

    public static SerdesMapper
    getInstance() { return instance; }
}

//////////////////////////////////////////////////////////////////////////////
