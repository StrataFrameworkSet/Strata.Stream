//////////////////////////////////////////////////////////////////////////////
// ChronicleMapUnboundedStreamSink.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.unbounded;

import net.openhft.chronicle.map.ChronicleMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import strata.stream.core.shared.IKeySelector;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public
class ChronicleMapUnboundedStreamSink<K,T>
    extends AbstractUnboundedStreamSink<T>
    implements AutoCloseable
{
    private Class<K> keyType;
    private Class<T> valueType;
    private String mapName;
    private String filePath;
    private Double averageValueSize;
    private IKeySelector<K,T> selector;
    private transient ChronicleMap<K,T> store;
    private final Logger logger;

    public ChronicleMapUnboundedStreamSink(
        Class<K> kType,
        Class<T> vType,
        String name,
        String path,
        Double avgValSize,
        IKeySelector<K,T> sel) throws IOException
    {
        keyType = kType;
        valueType = vType;
        mapName = name;
        filePath = path;
        averageValueSize = avgValSize;
        selector = sel;
        store =
            ChronicleMap
                .of(keyType,valueType)
                .name(mapName)
                .averageValueSize(averageValueSize)
                .entries(50)
                .createPersistedTo(new File(filePath));
        logger = LogManager.getLogger(this.getClass());
    }


    @Override
    protected void
    process(T element)
    {
        logger.debug("process({})",element);
        store.put(selector.getKey(element),element);
    }

    @Override
    public void
    close()
    {
        if (store != null)
            store.close();
    }

    public ChronicleMap<K,T>
    getStore()
    {
        return store;
    }



    private void
    writeObject(ObjectOutputStream output) throws IOException
    {
        output.defaultWriteObject();
    }

    private void
    readObject(ObjectInputStream input) throws IOException, ClassNotFoundException
    {
        input.defaultReadObject();
        store =
            ChronicleMap
                .of(keyType,valueType)
                .name(mapName)
                .averageValueSize(averageValueSize)
                .entries(10)
                .createPersistedTo(new File(filePath));

    }
}

//////////////////////////////////////////////////////////////////////////////
