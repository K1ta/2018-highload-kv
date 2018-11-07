package ru.mail.polis.K1ta.utils;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class ValueSerializer {

    public byte[] serialize(@NotNull Value value) {
        byte[] state = value.getState().name().getBytes();
        int length = 12 + state.length + value.getData().length; //12 = long size + int size
        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.putLong(value.getTimestamp());
        buffer.putInt(state.length);
        buffer.put(state);
        byte[] data = value.getData();
        buffer.put(data);
        return buffer.array();
    }

    public Value deserialize(byte[] serializedValue) {
        ByteBuffer buffer = ByteBuffer.wrap(serializedValue);
        long timestamp = buffer.getLong();
        int stateLength = buffer.getInt();
        byte[] state = new byte[stateLength];
        buffer.get(state);
        String stateValue = new String(state);
        byte[] value = new byte[serializedValue.length - 12 - stateLength];
        buffer.get(value);
        return new Value(value, timestamp, Value.stateCode.valueOf(stateValue));
    }
}
