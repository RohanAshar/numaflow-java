package io.numaproj.numaflow.flatmapper;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.flatmap.v1.FlatmapGrpc;
import io.numaproj.numaflow.flatmap.v1.FlatmapOuterClass;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class ServerTest {
    private final static String PROCESSED_KEY_SUFFIX = "-key-processed";
    private final static String PROCESSED_VALUE_SUFFIX = "-value-processed";
    private final static String key = "key";

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private Server server;
    private ManagedChannel inProcessChannel;

    @Before
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();

        GRPCConfig grpcServerConfig = GRPCConfig.newBuilder()
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(Constants.DEFAULT_SOCKET_PATH)
                .infoFilePath("/tmp/numaflow-test-server-info)")
                .build();

        server = new Server(
                new ServerTest.TestMapFn(),
                grpcServerConfig);

        server.setServerBuilder(InProcessServerBuilder.forName(serverName)
                .directExecutor());

        server.start();

        inProcessChannel = grpcCleanup.register(InProcessChannelBuilder
                .forName(serverName)
                .directExecutor()
                .build());
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }


    @Test
    public void testMapStreamHappyPath() {
        FlatMapOutputStreamObserver outputStreamObserver = new FlatMapOutputStreamObserver();
        StreamObserver<FlatmapOuterClass.MapRequest> inputStreamObserver = FlatmapGrpc
                .newStub(inProcessChannel)
                .mapFn(outputStreamObserver);

        for (int i =1; i <= 10; i++) {
            String message = i + "," + String.valueOf(i+10);
            FlatmapOuterClass.MapRequest request = FlatmapOuterClass.MapRequest.newBuilder()
                    .setValue(ByteString.copyFromUtf8(message))
                    .addKeys(key)
                    .setUuid(UUID.randomUUID().toString())
                    .build();
            inputStreamObserver.onNext(request);
        }

        while (outputStreamObserver.resultDatum.get().size() != 30) ;
        inputStreamObserver.onCompleted();
        List<FlatmapOuterClass.MapResponse> result = outputStreamObserver.resultDatum.get();
        // We expect 30 responses. We send 10 Inputs, each one having 2 outputs and 1 EOR msg
        assertEquals(30, result.size());

    }

    private static class TestMapFn extends FlatMapper {
        @Override
        public MessageList processMessage(String[] keys, Datum datum) {
            String[] updatedKeys = Arrays
                    .stream(keys)
                    .map(c -> c + PROCESSED_KEY_SUFFIX)
                    .toArray(String[]::new);
            String msg = new String(datum.getValue());
            String[] strs = msg.split(",");
            MessageList.MessageListBuilder listBuilder = MessageList.newBuilder();
            for (String str : strs) {
                listBuilder.addMessage(new Message(str.getBytes()));
            }
            return listBuilder.build();

        }
    }
}
