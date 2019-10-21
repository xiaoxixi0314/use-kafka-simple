package com.github.xiaoxixi.selfserial;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import java.nio.*;


public class DemoUserSerializer implements Serializer<DemoUser> {

    @Override
    public byte[] serialize(String topic, DemoUser demoUser) {
        if (demoUser == null) {
            return null;
        }

        try {
            byte[] nameByte;
            int nameSize = 0;
            Integer id = demoUser.getId();
            String name = demoUser.getName();
            if (name != null) {
                nameByte = name.getBytes("UTF-8");
                nameSize = name.length();
            } else {
                nameByte = new byte[0];
                nameSize = 0;
            }
            // 4 + 4 : id + name所占位数
            ByteBuffer buffer = ByteBuffer.allocate(4+4+nameSize);
            buffer.putInt(id);
            buffer.putInt(nameSize);
            buffer.put(nameByte);
            return buffer.array();
        }catch (Exception e) {
            throw new SerializationException("serial demo user error"+e);
        }
    }
}
