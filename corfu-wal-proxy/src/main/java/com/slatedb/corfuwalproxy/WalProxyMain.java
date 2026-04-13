package com.slatedb.corfuwalproxy;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import java.net.InetSocketAddress;

/**
 * Entry point for the corfu-wal-proxy sidecar.
 *
 * Usage:
 *   java -jar corfu-wal-proxy.jar \
 *       --corfu localhost:9000 \
 *       --stream slatedb-wal \
 *       --listen 127.0.0.1:50111
 */
public final class WalProxyMain {

    public static void main(String[] args) throws Exception {
        String corfuEndpoint = "localhost:9000";
        String streamName = "slatedb-wal";
        String listenHost = "127.0.0.1";
        int listenPort = 50111;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--corfu":
                    corfuEndpoint = args[++i];
                    break;
                case "--stream":
                    streamName = args[++i];
                    break;
                case "--listen":
                    String[] hp = args[++i].split(":");
                    listenHost = hp[0];
                    listenPort = Integer.parseInt(hp[1]);
                    break;
                default:
                    System.err.println("unknown arg: " + args[i]);
                    System.exit(2);
            }
        }

        WalProxyService service = new WalProxyService(corfuEndpoint, streamName);
        Server server = NettyServerBuilder
                .forAddress(new InetSocketAddress(listenHost, listenPort))
                .addService(service)
                .build()
                .start();

        System.out.println("corfu-wal-proxy listening on " + listenHost + ":" + listenPort
                + " backed by " + corfuEndpoint + " stream=" + streamName);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("shutting down corfu-wal-proxy");
            service.close();
            server.shutdown();
        }));

        server.awaitTermination();
    }

    private WalProxyMain() {}
}
