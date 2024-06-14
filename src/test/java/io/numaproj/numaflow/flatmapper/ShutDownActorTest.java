package io.numaproj.numaflow.flatmapper;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AllDeadLetters;
import akka.actor.DeadLetter;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.flatmap.v1.FlatmapGrpc;
import io.numaproj.numaflow.flatmap.v1.FlatmapOuterClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class ShutDownActorTest {

    @Test
    public void testFailure() {
        final ActorSystem actorSystem = ActorSystem.create("test-system-1");
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        FlatmapOuterClass.MapRequest request = FlatmapOuterClass.MapRequest.newBuilder()
                .setValue(ByteString.copyFromUtf8("message"))
                .addKeys("key")
                .setUuid(UUID.randomUUID().toString())
                .build();

        ActorRef shutdownActor = actorSystem
                .actorOf(FlatMapShutdownActor
                        .props(completableFuture));

        FlatMapOutputStreamObserver outputStreamObserver = new FlatMapOutputStreamObserver();
        ActorRef flatMapActor = actorSystem.actorOf(FlatMapActor.props(new TestMapFn(), shutdownActor, outputStreamObserver));
        flatMapActor.tell(request, ActorRef.noSender());


        try {
            completableFuture.get();
            fail("Expected the future to complete with exception");
        } catch (Exception e) {
            assertEquals(e.getMessage(), "java.lang.RuntimeException: unknown exception");
        }
    }

    @Test
    public void testDeadLetterHandling() {
        final ActorSystem actorSystem = ActorSystem.create("test-system-2");
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        FlatmapOuterClass.MapRequest request = FlatmapOuterClass.MapRequest.newBuilder()
                .setValue(ByteString.copyFromUtf8("message"))
                .addKeys("key")
                .setUuid(UUID.randomUUID().toString())
                .build();

        ActorRef shutdownActor = actorSystem
                .actorOf(FlatMapShutdownActor
                        .props(completableFuture));

        actorSystem.eventStream().subscribe(shutdownActor, AllDeadLetters.class);

        DeadLetter deadLetter = new DeadLetter("dead-letter", shutdownActor, shutdownActor);

        FlatMapOutputStreamObserver outputStreamObserver = new FlatMapOutputStreamObserver();
        ActorRef flatMapActor = actorSystem.actorOf(FlatMapActor.props(new TestMapFn(), shutdownActor, outputStreamObserver));
        flatMapActor.tell(deadLetter, ActorRef.noSender());


        try {
            completableFuture.get();
            fail("Expected the future to complete with exception");
        } catch (Exception e) {
            assertEquals(e.getMessage(), "java.lang.Throwable: dead letters");
        }
    }

    private static class TestMapFn extends FlatMapper {
        @Override
        public MessageList processMessage(String[] keys, Datum datum) {
            throw new RuntimeException("unknown exception");
        }
    }
}
