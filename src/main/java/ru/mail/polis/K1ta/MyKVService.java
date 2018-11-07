package ru.mail.polis.K1ta;

import jdk.nashorn.internal.runtime.regexp.joni.exception.InternalException;
import one.nio.http.*;
import one.nio.net.ConnectionString;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.K1ta.utils.ReplicaInfo;
import ru.mail.polis.K1ta.utils.Value;
import ru.mail.polis.K1ta.utils.ValueSerializer;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class MyKVService extends HttpServer implements KVService {
    private final KVDao dao;
    private final String[] topology;
    private final Map<String, HttpClient> nodes;
    private final String me;
    private final ValueSerializer serializer;
    private final Logger logger;

    public MyKVService(final int port, KVDao dao, Set<String> topology) throws IOException {
        super(getConfig(port));
        this.dao = dao;
        this.topology = topology.toArray(new String[0]);
        me = "http://localhost:" + port;
        nodes = topology.stream().collect(Collectors.toMap(
                o -> o,
                o -> new HttpClient(new ConnectionString(o))));
        serializer = new ValueSerializer();
        logger = Logger.getLogger(this.getClass().getName());
        System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tT %4$s %2$s %5$s%6$s%n");
    }

    private static HttpServerConfig getConfig(int port) {
        HttpServerConfig serverConfig = new HttpServerConfig();
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        serverConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        return serverConfig;
    }

    @Path("/v0/status")
    public Response status(Request request) {
        logger.info("me = " + me + " Request on " + request.getHost() + request.getURI() + " type=" + request.getMethod());
        return new Response(Response.OK, Response.EMPTY);
    }

    @Path("/v0/entity")
    public Response entity(
            Request request,
            @Param(value = "id") String id,
            @Param(value = "replicas") String replicas) {
        logger.info("me = " + me + " Request on " + request.getHost() + request.getURI() + " type=" + request.getMethod());

        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        logger.info("me = " + me + " id=" + id);

        ReplicaInfo replicaInfo;
        try {
            replicaInfo = new ReplicaInfo(replicas, topology.length);
        } catch (IllegalArgumentException e) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        logger.info("me = " + me + " ack=" + replicaInfo.getAck() + " from=" + replicaInfo.getFrom());

        boolean proxied = request.getHeader("proxied") != null;

        logger.info("me = " + me + " proxied=" + proxied);

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                logger.info("case GET");
                return proxied ?
                        get(id) :
                        proxyGet(id, replicaInfo.getAck(), getNodes(id, replicaInfo.getFrom()));
            case Request.METHOD_PUT:
                logger.info("case PUT");
                return proxied ?
                        put(id, request.getBody()) :
                        proxyPut(id, request.getBody(), replicaInfo.getAck(), getNodes(id, replicaInfo.getFrom()));
            case Request.METHOD_DELETE:
                logger.info("case DELETE");
                return proxied ?
                        delete(id) :
                        proxyDelete(id, replicaInfo.getAck(), getNodes(id, replicaInfo.getFrom()));
        }

        logger.info("me = " + me + " Method not allowed");
        return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
    }

    private Response proxyGet(String id, int ack, List<String> from) {
        logger.info("me = " + me + " proxyGet with id=" + id);
        List<Value> values = new ArrayList<>();

        for (String node : from) {
            if (node.equals(me)) {
                try {
                    byte[] res = dao.get(id.getBytes());
                    Value resValue = serializer.deserialize(res);
                    values.add(resValue);
                } catch (NoSuchElementException e) {
                    logger.log(Level.INFO, "No such element");
                    values.add(new Value(Value.EMPTY_DATA, 0, Value.stateCode.UNKNOWN));
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "IO exception", e);
                }
            } else {
                try {
                    values.add(internalGet(id, node));
                } catch (NumberFormatException e) {
                    logger.log(Level.SEVERE, "wrong type of headers", e);
                } catch (InternalException e) {
                    logger.log(Level.SEVERE, "internal error on node " + node, e);
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "bad answer, no ack", e);
                }
            }
        }
        if (values.stream().anyMatch(v -> v.getState() == Value.stateCode.DELETED)) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        List<Value> present = values.stream()
                .filter(v -> v.getState() == Value.stateCode.PRESENT || v.getState() == Value.stateCode.UNKNOWN)
                .collect(Collectors.toList());
        if (present.size() >= ack) {
            logger.info("SUCCESS, " + values.size() + "/" + from.size());
            Value max = present.stream()
                    .max(Comparator.comparingLong(Value::getTimestamp)).get();
            return max.getState() == Value.stateCode.UNKNOWN ?
                    new Response(Response.NOT_FOUND, Response.EMPTY) :
                    new Response(Response.OK, max.getData());
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private Value internalGet(String id, String node) throws Exception {
        final Response response = nodes.get(node).get("/v0/entity?id=" + id, "proxied: true");
        if (response.getStatus() == 500) {
            throw new InternalException("Not enough acks");
        }
        if (response.getStatus() != 404) {
            byte[] res = response.getBody();
            String timestampHeader = response.getHeader("Timestamp");
            long timestamp = Long.parseLong(timestampHeader);
            String state = response.getHeader("State");
            return new Value(res, timestamp, Value.stateCode.valueOf(state));
        } else {
            return new Value(Value.EMPTY_DATA, 0, Value.stateCode.UNKNOWN);
        }
    }

    private Response get(String id) {
        logger.info("me = " + me + " get with id=" + id);
        Response response;
        try {
            byte[] res = dao.get(id.getBytes());
            Value value = serializer.deserialize(res);
            response = new Response(Response.OK, value.getData());
            response.addHeader("Timestamp" + value.getTimestamp());
            response.addHeader("State" + value.getState());
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        return response;
    }

    private Response proxyPut(String id, byte[] value, int ack, List<String> from) {
        logger.info("me = " + me + " proxyPut with id=" + id);
        int myAck = 0;

        for (String node : from) {
            if (node.equals(me)) {
                Value val = new Value(value, System.currentTimeMillis());
                try {
                    byte[] ser = serializer.serialize(val);
                    dao.upsert(id.getBytes(), ser);
                    myAck++;
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "IOException with id=" + id + " me=" + me, e);
                }
            } else {
                try {
                    final Response response = nodes.get(node).put("/v0/entity?id=" + id, value, "proxied: true");
                    if (response.getStatus() != 500) {
                        myAck++;
                    }
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "bad answer, no ack + me=" + me, e);
                }
            }
        }

        if (myAck >= ack) {
            logger.info("SUCCESS, " + myAck + "/" + from.size());
            return new Response(Response.CREATED, Response.EMPTY);
        }
        logger.info("ERROR, " + myAck + "/" + from.size());
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private Response put(String id, byte[] value) {
        logger.info("me = " + me + " put with id=" + id);
        try {
            Value val = new Value(value, System.currentTimeMillis());
            dao.upsert(id.getBytes(), serializer.serialize(val));
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response proxyDelete(String id, int ack, List<String> from) {
        logger.info("me = " + me + " proxyDelete with id=" + id);
        int myAck = 0;

        for (String node : from) {
            if (node.equals(me)) {
                Value val = new Value(Value.EMPTY_DATA, System.currentTimeMillis(), Value.stateCode.DELETED);
                try {
                    byte[] ser = serializer.serialize(val);
                    dao.upsert(id.getBytes(), ser);
                    myAck++;
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "IOException with id=" + id + " me=" + me, e);
                }
            } else {
                try {
                    Value val = new Value(Value.EMPTY_DATA, System.currentTimeMillis(), Value.stateCode.DELETED);
                    byte[] value = serializer.serialize(val);
                    final Response response = nodes.get(node).put("/v0/entity?id=" + id, value, "proxied: true");
                    if (response.getStatus() != 500) {
                        myAck++;
                    }
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "bad answer, no ack, me=" + me, e);
                }
            }
        }

        if (myAck >= ack) {
            logger.info("SUCCESS, " + myAck + "/" + from.size());
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }
        logger.info("ERROR, " + myAck + "/" + from.size());
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private Response delete(String id) {
        logger.info("me = " + me + " delete with id=" + id);
        try {
            Value val = new Value(Value.EMPTY_DATA, System.currentTimeMillis(), Value.stateCode.DELETED);
            dao.upsert(id.getBytes(), serializer.serialize(val));
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    private List<String> getNodes(String key, int length) {
        final List<String> clients = new ArrayList<>();
        //сгенерировать номер ноды на основе hash(key)
        int firstNodeId = (key.hashCode() & Integer.MAX_VALUE) % topology.length;
        clients.add(topology[firstNodeId]);
        //в цикле на увеличение добавить туда еще нод
        for (int i = 1; i < length; i++) {
            clients.add(topology[(firstNodeId + i) % topology.length]);
        }
        return clients;
    }

}
