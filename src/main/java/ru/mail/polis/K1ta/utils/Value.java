package ru.mail.polis.K1ta.utils;

import java.io.Serializable;

public class Value implements Serializable {

    public byte[] data;
    private long timestamp;
    private int state;

    public static final byte[] EMPTY_DATA = new byte[0];
    public static int PRESENT = 0;
    public static int DELETED = 1;
    public static int UNKNOWN = 2;

    /*public Value() {
        data = EMPTY_DATA;
        timestamp = 0;
        state = UNKNOWN;
    }*/

    public Value(byte[] value, long timestamp) {
        this.data = value;
        this.timestamp = timestamp;
        state = PRESENT;
    }

    public Value(byte[] value, long timestamp, int state) {
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

    public int getState() {
        return state;
    }

}
