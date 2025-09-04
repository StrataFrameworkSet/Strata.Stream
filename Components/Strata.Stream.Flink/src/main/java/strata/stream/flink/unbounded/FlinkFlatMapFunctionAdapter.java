//////////////////////////////////////////////////////////////////////////////
// FlinkFlatMapFunctionAdapter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;
import strata.stream.core.shared.IFunction;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public
class FlinkFlatMapFunctionAdapter<T,R>
    implements FlatMapFunction<T,R>,ResultTypeQueryable<R>
{
    private final IFunction<T,Iterable<R>> mapper;
    private static Map<String,Class<?>>    types = new ConcurrentHashMap<>();

    public
    FlinkFlatMapFunctionAdapter(IFunction<T,Iterable<R>> m)
    {
        mapper = m;
    }

    @Override
    public void
    flatMap(T value,Collector<R> out) throws Exception
    {
        mapper
            .apply(value)
            .forEach(e -> process(e,out));
    }


    private void
    process(R value,Collector<R> out)
    {
        out.collect(value);
    }

    @Override
    public TypeInformation<R>
    getProducedType()
    {
        return TypeInformation.of(getReturnType(mapper).get());
    }

    public static <T,R> void
    registerReturnType(IFunction<T,Iterable<R>> mapper,Class<R> type)
    {
        String key = getKey(mapper);

        //System.out.println("Register return type with key: <<<" + key + ">>>");
        if (!types.containsKey(key))
            types.put(key,type);
    }

    @SuppressWarnings("unchecked")
    public static <T,R> Optional<Class<R>>
    getReturnType(IFunction<T,Iterable<R>> mapper)
    {
        String key = getKey(mapper);

        //System.out.println("Get return type with key: <<<" + key + ">>>");
        return Optional.of((Class<R>)types.get(key));
    }

    private static <T,R> String
    getKey(IFunction<T,Iterable<R>> mapper)
    {
       return
           mapper
               .getClass()
               .getGenericSuperclass()
               .getTypeName();
    }

    private static String
    concatenate(Type[] parameters)
    {
        StringBuilder builder = new StringBuilder();
        Arrays
            .stream(parameters)
            .forEach(p -> builder.append(p.getTypeName()).append('|'));

        return
            builder
                .append("end")
                .toString();
    }
}

//////////////////////////////////////////////////////////////////////////////
