package ru.mail.polis.K1ta;

import one.nio.http.*;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.util.NoSuchElementException;

public class MyKVService extends HttpServer implements KVService {
    private final KVDao dao;

    public MyKVService(HttpServerConfig config, KVDao dao) throws IOException {
        super(config);
        this.dao = dao;
    }

    @Path("/v0/status")
    public Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }

    @Path("/v0/entity")
    public Response entity(Request request, @Param(value = "id") String id) {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(id);
            case Request.METHOD_PUT:
                return put(id, request.getBody());
            case Request.METHOD_DELETE:
                return delete(id);
        }
        return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
    }

    private Response get(String id) {
        byte[] res;
        try {
            res = dao.get(id.getBytes());
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        return new Response(Response.OK, res);
    }

    private Response put(String id, byte[] value) {
        try {
            dao.upsert(id.getBytes(), value);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response delete(String id) {
        try {
            dao.remove(id.getBytes());
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

}