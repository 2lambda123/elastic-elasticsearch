/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package fixture.s3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Objects;

public class S3HttpFixture {

    private final HttpServer server;

    S3HttpFixture(final String[] args) throws Exception {
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(args[0]), Integer.parseInt(args[1])), 0);
        this.server.createContext("/", Objects.requireNonNull(createHandler(args)));
    }

    final void start() throws Exception {
        try {
            server.start();
            // wait to be killed
            Thread.sleep(Long.MAX_VALUE);
        } finally {
            server.stop(0);
        }
    }

    protected HttpHandler createHandler(final String[] args) {
        final String bucket = Objects.requireNonNull(args[2]);
        final String basePath = args[3];
        final String accessKey = Objects.requireNonNull(args[4]);

        return new S3HttpHandler(bucket, basePath) {
            @Override
            public void handle(final HttpExchange exchange) throws IOException {
                final String authorization = exchange.getRequestHeaders().getFirst("Authorization");
                if (authorization == null || authorization.contains(accessKey) == false) {
                    sendError(exchange, RestStatus.FORBIDDEN, "AccessDenied", "Bad access key");
                    return;
                }
                super.handle(exchange);
            }
        };
    }

    public static void main(final String[] args) throws Exception {
        if (args == null || args.length < 5) {
            throw new IllegalArgumentException("S3HttpFixture expects 5 arguments [address, port, bucket, base path, access key]");
        }
        final S3HttpFixture fixture = new S3HttpFixture(args);
        fixture.start();
    }
}
