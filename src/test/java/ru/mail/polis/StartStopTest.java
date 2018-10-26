/*
 * Copyright 2018 (c) Vadim Tsesko <incubos@yandex.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis;

import one.nio.http.HttpClient;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic init/deinit test for {@link KVService} implementation
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
class StartStopTest extends TestBase {
    private static final long TIMEOUT_MS = TimeUnit.SECONDS.toMillis(1);
    private static final Duration TIMEOUT = Duration.ofMillis(TIMEOUT_MS * 2);

    private int port;
    private File data;
    private KVDao dao;
    private KVService kvService;
    private HttpClient client;

    @BeforeEach
    void beforeEach() throws IOException {
        data = Files.createTempDirectory();
        dao = KVDaoFactory.create(data);
        port = randomPort();
        kvService = KVServiceFactory.create(port, dao, Collections.singleton(endpoint(port)));
        reset();
    }

    @AfterEach
    void afterEach() throws IOException {
        client.close();
        kvService.stop();
        dao.close();
        Files.recursiveDelete(data);
    }

    private int status() throws Exception {
        return client.get("/v0/status").getStatus();
    }

    private void reset() {
        if (client != null) {
            client.close();
        }
        client = new HttpClient(new ConnectionString("http://localhost:" + port));
    }

    @Test
    void create() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            assertThrows(PoolException.class, this::status);
        });
    }

    @Test
    void start() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            kvService.start();
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));

            assertEquals(200, status());
        });
    }

    @Test
    void stop() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            kvService.start();
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));

            assertEquals(200, status());

            kvService.stop();
            reset();

            // Should not respond after stop
            assertThrows(PoolException.class, this::status);
        });
    }
}
