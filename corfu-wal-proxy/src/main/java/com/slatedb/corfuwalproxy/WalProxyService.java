package com.slatedb.corfuwalproxy;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.view.stream.IStreamView;
import slatedb.wal_proxy.v1.WalProxyOuterClass.AppendRequest;
import slatedb.wal_proxy.v1.WalProxyOuterClass.AppendResponse;
import slatedb.wal_proxy.v1.WalProxyOuterClass.ListRequest;
import slatedb.wal_proxy.v1.WalProxyOuterClass.ListResponse;
import slatedb.wal_proxy.v1.WalProxyOuterClass.ReadEntry;
import slatedb.wal_proxy.v1.WalProxyOuterClass.ReadRequest;
import slatedb.wal_proxy.v1.WalProxyOuterClass.TailRequest;
import slatedb.wal_proxy.v1.WalProxyOuterClass.TailResponse;
import slatedb.wal_proxy.v1.WalProxyOuterClass.TrimRequest;
import slatedb.wal_proxy.v1.WalProxyOuterClass.TrimResponse;
import slatedb.wal_proxy.v1.WalProxyGrpc;

import java.util.UUID;

/**
 * gRPC service that translates {@link slatedb.wal_proxy.v1.WalProxyGrpc}
 * calls into operations on a Corfu {@link IStreamView}.
 *
 * <p>This is intentionally a thin wrapper. The Java side does no payload
 * inspection — every WAL is an opaque {@code byte[]} that the Rust client
 * encoded with its own row codec. Corfu's sequencer assigns the global log
 * address that we return to the client as the WAL id.
 *
 * <p>The implementation reuses the existing {@code site.ycsb.db.corfu.CorfuBridge}
 * Java wrapper for streamView.append/poll where convenient, but for the
 * read/list paths we drive {@link IStreamView} directly because the bridge
 * doesn't expose seek-by-address.
 */
public class WalProxyService extends WalProxyGrpc.WalProxyImplBase implements AutoCloseable {

    private final CorfuRuntime runtime;
    private final IStreamView streamView;

    public WalProxyService(String corfuEndpoint, String streamName) {
        CorfuRuntimeParameters params = CorfuRuntimeParameters.builder().build();
        this.runtime = CorfuRuntime.fromParameters(params)
                .parseConfigurationString(corfuEndpoint)
                .connect();
        UUID streamId = CorfuRuntime.getStreamID(streamName);
        this.streamView = runtime.getStreamsView().get(streamId);
    }

    @Override
    public void append(AppendRequest req, StreamObserver<AppendResponse> resp) {
        try {
            byte[] payload = req.getPayload().toByteArray();
            long address = streamView.append(payload);
            resp.onNext(AppendResponse.newBuilder().setAddress(address).build());
            resp.onCompleted();
        } catch (Throwable t) {
            resp.onError(t);
        }
    }

    @Override
    public void read(ReadRequest req, StreamObserver<ReadEntry> resp) {
        try {
            // Reset the stream view to just past `after_addr` and walk forward.
            // IStreamView.seek positions the view so the next `next()` returns
            // the entry immediately after the supplied address.
            streamView.seek(req.getAfterAddr() + 1);
            while (true) {
                ILogData data = streamView.next();
                if (data == null) {
                    break;
                }
                Object raw = data.getPayload(runtime);
                if (!(raw instanceof byte[])) {
                    // Skip entries written by other clients.
                    continue;
                }
                ReadEntry out = ReadEntry.newBuilder()
                        .setAddress(data.getGlobalAddress())
                        .setPayload(ByteString.copyFrom((byte[]) raw))
                        .build();
                resp.onNext(out);
            }
            resp.onCompleted();
        } catch (Throwable t) {
            resp.onError(t);
        }
    }

    @Override
    public void list(ListRequest req, StreamObserver<ListResponse> resp) {
        try {
            ListResponse.Builder out = ListResponse.newBuilder();
            streamView.seek(req.getAfterAddr() + 1);
            while (true) {
                ILogData data = streamView.next();
                if (data == null) {
                    break;
                }
                out.addAddresses(data.getGlobalAddress());
            }
            resp.onNext(out.build());
            resp.onCompleted();
        } catch (Throwable t) {
            resp.onError(t);
        }
    }

    @Override
    public void tail(TailRequest req, StreamObserver<TailResponse> resp) {
        try {
            long tail = runtime.getSequencerView().query().getSequence();
            resp.onNext(TailResponse.newBuilder().setAddress(tail).build());
            resp.onCompleted();
        } catch (Throwable t) {
            resp.onError(t);
        }
    }

    @Override
    public void trim(TrimRequest req, StreamObserver<TrimResponse> resp) {
        try {
            // Best-effort prefix trim. Corfu's runtime exposes prefixTrim on
            // the address space view; the Token wraps (epoch, sequence).
            org.corfudb.protocols.wireprotocol.Token token =
                    new org.corfudb.protocols.wireprotocol.Token(runtime.getLayoutView().getLayout().getEpoch(), req.getUpTo());
            runtime.getAddressSpaceView().prefixTrim(token);
            runtime.getAddressSpaceView().gc();
            resp.onNext(TrimResponse.newBuilder().build());
            resp.onCompleted();
        } catch (Throwable t) {
            resp.onError(t);
        }
    }

    @Override
    public void close() {
        try {
            runtime.shutdown();
        } catch (Throwable ignored) {
            // Best-effort shutdown.
        }
    }
}
