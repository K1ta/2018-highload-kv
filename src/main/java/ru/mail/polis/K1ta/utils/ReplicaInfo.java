package ru.mail.polis.K1ta.utils;

public class ReplicaInfo {

    private int ack;
    private int from;

    public ReplicaInfo(String replicas, int topologyLength) {
        if (replicas == null) {
            ack = topologyLength / 2 + 1;
            from = topologyLength;
            return;
        }
        String parts[] = replicas.split("/");
        if (parts.length != 2) {
            throw new IllegalArgumentException();
        }
        ack = Integer.parseInt(parts[0]);
        from = Integer.parseInt(parts[1]);
        if (ack <= 0 || ack > from) {
            throw new IllegalArgumentException();
        }
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }
}
