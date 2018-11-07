package ru.mail.polis.K1ta.utils;

import java.io.Serializable;
import java.util.Date;

public class Value implements Serializable {

    public byte[] data;
    private long timestamp;
    private stateCode state;

    public static final byte[] EMPTY_DATA = new byte[0];

    public enum stateCode {
        PRESENT,
        DELETED,
        UNKNOWN
    }

    public Value() {
        this.data = EMPTY_DATA;
        this.timestamp = System.currentTimeMillis();
        this.state = stateCode.DELETED;
    }

    public Value(byte[] value) {
        this.data = value;
        this.timestamp = System.currentTimeMillis();
        state = stateCode.PRESENT;
    }

    public Value(byte[] value, long timestamp, stateCode state) {
        this.data = value;
        this.timestamp = timestamp;
        this.state = state;
    }

    public byte[] getData() {
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public stateCode getState() {
        return state;
    }

    @Override
    public String toString() {
        return "value[timestamp: " + new Date(timestamp) + " state: " + state.name() + "]";
    }

}
