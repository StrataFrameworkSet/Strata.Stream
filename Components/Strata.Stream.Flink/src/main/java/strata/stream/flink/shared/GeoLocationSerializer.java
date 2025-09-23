/// ///////////////////////////////////////////////////////////////////////////
// GeoLocationSerializer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.shared;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import strata.foundation.core.value.GeoLocation;

public
class GeoLocationSerializer
    extends Serializer<GeoLocation>
{
    @Override
    public void
    write(Kryo kryo,Output output,GeoLocation geoLocation)
    {
        output.writeDouble(geoLocation.getLatitude());
        output.writeDouble(geoLocation.getLongitude());
    }

    @Override
    public GeoLocation
    read(Kryo kryo,Input input,Class<GeoLocation> type)
    {
        return GeoLocation.of(input.readDouble(),input.readDouble());
    }
}

//////////////////////////////////////////////////////////////////////////////
