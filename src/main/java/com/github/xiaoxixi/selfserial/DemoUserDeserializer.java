package com.github.xiaoxixi.selfserial;

import com.sun.xml.internal.ws.encoding.soap.DeserializationException;
import org.apache.kafka.common.serialization.Deserializer;
import java.nio.ByteBuffer;

public class DemoUserDeserializer implements Deserializer<DemoUser> {

    @Override
    public DemoUser deserialize(String topic, byte[] data) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int id = buffer.getInt();
            int nameSize = buffer.getInt();
            byte[] nameBytes = new byte[nameSize];
            buffer.get(nameBytes);
            String name = new String(nameBytes, "UTF-8");
            DemoUser user = new DemoUser();
            user.setId(id);
            user.setName(name);
            return user;
        } catch (Exception e) {
            throw new DeserializationException("deserialize error:" + e);
        }
    }
}
