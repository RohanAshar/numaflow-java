package io.numaproj.numaflow.flatmapper;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.flatmap.v1.FlatmapOuterClass;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Instant;

import static org.mockito.Mockito.mock;

public class FlatMapActorTest {

    @Test
    public void test_process_valid_map_request() {
        FlatMapper flatMapper = mock(FlatMapper.class);
        ActorRef shutdownActor = mock(ActorRef.class);
        StreamObserver<FlatmapOuterClass.MapResponse> responseObserver = mock(StreamObserver.class);

        // Create the actor system
        ActorSystem system = ActorSystem.create("test-system");

        // Create the FlatMapActor
        Props props = FlatMapActor.props(flatMapper, shutdownActor, responseObserver);

        TestActorRef<FlatMapActor> actorRef = TestActorRef.create(system, props);

        FlatmapOuterClass.MapRequest mapRequest = FlatmapOuterClass.MapRequest.newBuilder()
                .setValue(ByteString.copyFromUtf8("test"))
                .setWatermark(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                .setEventTime(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                .putHeaders("headerKey", "headerValue")
                .addKeys("key1")
                .setUuid("uuid")
                .build();

        Message message = new Message("response".getBytes(), new String[]{"key1"});
        MessageList messageList = MessageList.newBuilder().addMessage(message).build();
        Mockito.when(flatMapper.processMessage(Mockito.any(), Mockito.any())).thenReturn(messageList);

        actorRef.tell(mapRequest, ActorRef.noSender());

        Mockito.verify(responseObserver, Mockito.times(2)).onNext(Mockito.any(FlatmapOuterClass.MapResponse.class));
        Mockito.verify(responseObserver, Mockito.times(1)).onNext(Mockito.argThat(response -> response.getResult().getEOR()));
    }

}
