package org.corfudb.integration;



import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

public class DummySerializer implements ISerializer {

    private static final byte type = Serializers.SYSTEM_SERIALIZERS_COUNT + 1;

    @Override
    public byte getType() {
        return type;
    }

    @Override
    public Object deserialize(ByteBuf b, CorfuRuntime var2) {
        return Unpooled.copiedBuffer(b);
    }

    @Override
    public void serialize(Object o, ByteBuf byteBuf) {}
}