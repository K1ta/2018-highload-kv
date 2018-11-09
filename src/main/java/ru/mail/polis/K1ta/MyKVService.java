package ru.mail.polis.K1ta;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import one.nio.server.AcceptorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.K1ta.utils.RequestInfo;
import ru.mail.polis.K1ta.utils.Value;
import ru.mail.polis.K1ta.utils.ValueSerializer;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.util.*;
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
        serializer = ValueSerializer.getInstance();
        logger = LoggerFactory.getLogger(MyKVService.class);
        logger.info("Service created, me=" + me);
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
        logger.info(request.getURI());
        logger.debug(request.toString());
        return new Response(Response.OK, Response.EMPTY);
    }

    @Path("/v0/entity")
    public Response entity(
            Request request,
            @Param(value = "id") String id,
            @Param(value = "replicas") String replicas) {
        logger.info(request.getURI());
        logger.debug(request.toString());

        RequestInfo requestInfo;
        try {
            requestInfo = new RequestInfo(id, replicas, topology.length, request.getHeader("Proxied"));
        } catch (IllegalArgumentException e) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        logger.debug("id = " + requestInfo.getId());
        logger.debug("ack=" + requestInfo.getAck() + " from=" + requestInfo.getFrom());
        logger.info("Proxied=" + requestInfo.isProxied());

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                logger.debug("case GET");
                return requestInfo.isProxied() ?
                        get(id) :
                        proxyGet(id, requestInfo.getAck(), getNodes(id, requestInfo.getFrom()));
            case Request.METHOD_PUT:
                logger.debug("case PUT");
                return requestInfo.isProxied() ?
                        internalUpsert(id, new Value(request.getBody())) :
                        proxyUpsert(id, requestInfo.getAck(), getNodes(id, requestInfo.getFrom()), true, request.getBody());
            case Request.METHOD_DELETE:
                logger.debug("case DELETE");
                return requestInfo.isProxied() ?
                        internalUpsert(id, new Value()) :
                        proxyUpsert(id, requestInfo.getAck(), getNodes(id, requestInfo.getFrom()), false, Value.EMPTY_DATA);
        }

        logger.debug("Method not allowed");
        return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
    }

    private Response proxyGet(String id, int ack, List<String> from) {
        logger.info("id=" + id);
        List<Value> values = new ArrayList<>();
        for (String node : from) {
            if (node.equals(me)) {
                try {
                    byte[] res = dao.get(id.getBytes());
                    Value resValue = serializer.deserialize(res);
                    values.add(resValue);
                    logger.info("Add to list " + resValue.toString());
                } catch (NoSuchElementException e) {
                    logger.error("No such element", e);
                    values.add(Value.UNKNOWN);
                } catch (IOException e) {
                    logger.error("IO exception", e);
                }
            } else {
                try {
                    Value resValue = internalGet(id, node);
                    values.add(resValue);
                    logger.info("Add to list " + resValue.toString());
                } catch (NumberFormatException e) {
                    logger.error("Wrong type of headers", e);
                } catch (Exception e) {
                    logger.error("Bad answer, no ack", e);
                }
            }
        }
        if (values.size() >= ack) {
            logger.info("SUCCESS, " + values.size() + "/" + from.size());
            Value max = values.stream()
                    .max(Comparator.comparingLong(Value::getTimestamp))
                    .orElse(Value.UNKNOWN);
            switch (max.getState()) {
                case UNKNOWN:
                case DELETED:
                    return new Response(Response.NOT_FOUND, Response.EMPTY);
                case PRESENT:
                    return new Response(Response.OK, max.getData());
            }
        }
        logger.info("FAIL, " + values.size() + "/" + from.size());
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private Value internalGet(String id, String node) throws Exception {
        logger.info("id=" + id);
        final Response response = nodes.get(node).get("/v0/entity?id=" + id, "Proxied: true");
        switch (response.getStatus()) {
            case 500:
                throw new Exception("Internal error on node");
            case 404:
                return Value.UNKNOWN;
            default:
                byte[] res = response.getBody();
                String timestampHeader = response.getHeader("Timestamp");
                long timestamp = Long.parseLong(timestampHeader);
                String state = response.getHeader("State");
                return new Value(res, timestamp, Value.stateCode.valueOf(state));
        }
    }

    private Response get(String id) {
        logger.info("id=" + id);
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

    private Response proxyUpsert(String id, int ack, List<String> from, boolean put, byte[] value) {
        logger.info("id=" + id + " type=" + (put ? "PUT" : "DELETE"));
        int myAck = 0;

        Value val = put ? new Value(value) : new Value();
        for (String node : from) {
            if (node.equals(me)) {
                try {
                    dao.upsert(id.getBytes(), serializer.serialize(val));
                    myAck++;
                    logger.info("Get ack from " + node);
                } catch (IOException e) {
                    logger.error("IOException with id=" + id, e);
                }
            } else {
                try {
                    final Response response = put ?
                            nodes.get(node).put("/v0/entity?id=" + id, value, "Proxied: true") :
                            nodes.get(node).delete("/v0/entity?id=" + id, "Proxied: true");
                    if (response.getStatus() != 500) {
                        myAck++;
                        logger.info("Get ack from " + node);
                    }
                } catch (Exception e) {
                    logger.error("Bad answer, no ack from node " + node, e);
                }
            }
        }

        if (myAck >= ack) {
            logger.info("SUCCESS, " + myAck + "/" + from.size());
            return put ?
                    new Response(Response.CREATED, Response.EMPTY) :
                    new Response(Response.ACCEPTED, Response.EMPTY);
        }
        logger.info("FAIL, " + myAck + "/" + from.size());
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private Response internalUpsert(String id, Value value) {
        logger.info("id=" + id);
        try {
            dao.upsert(id.getBytes(), serializer.serialize(value));
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        return new Response(Response.CREATED, Response.EMPTY);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        logger.info(request.getURI());
        logger.debug(request.toString());
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
