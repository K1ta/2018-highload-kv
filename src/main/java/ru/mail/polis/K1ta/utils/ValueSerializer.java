package ru.mail.polis.K1ta.utils;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class ValueSerializer {

    public byte[] serialize(@NotNull Value value) {
        int length = 12 + value.getData().length; //12 = long size + int size
        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.putLong(value.getTimestamp());
        buffer.putInt(value.getState());
        buffer.put(value.getData());
        return buffer.array();
    }

    public Value deserialize(byte[] serializedValue) {
        ByteBuffer buffer = ByteBuffer.wrap(serializedValue);
        long timestamp = buffer.getLong();
        int state = buffer.getInt();
        byte[] value = new byte[serializedValue.length - 12];
        buffer.get(value);
        return new Value(value, timestamp, state);
    }
}
