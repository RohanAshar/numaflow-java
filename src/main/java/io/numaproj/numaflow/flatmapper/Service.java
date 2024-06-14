package io.numaproj.numaflow.flatmapper;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AllDeadLetters;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.flatmap.v1.FlatmapGrpc;
import io.numaproj.numaflow.flatmap.v1.FlatmapOuterClass;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;



@Slf4j
@AllArgsConstructor
class Service extends FlatmapGrpc.FlatmapImplBase {

    private final FlatMapper flatMapper;
    public static final ActorSystem flatMapActorSystem = ActorSystem.create("flatmap");



    static void handleFailure(
            CompletableFuture<Void> failedFuture,
            StreamObserver<FlatmapOuterClass.MapResponse> responseStreamObserver) {
        new Thread(() -> {
            try {
                failedFuture.get();
            } catch (Exception e) {
                e.printStackTrace();
                var status = Status.UNKNOWN.withDescription(e.getMessage()).withCause(e);
                responseStreamObserver.onError(status.asException());
            }
        }).start();

    }

    @Override
    public StreamObserver<FlatmapOuterClass.MapRequest> mapFn(StreamObserver<FlatmapOuterClass.MapResponse> responseObserver) {

        if (this.flatMapper == null) {
            return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(
                    FlatmapGrpc.getMapFnMethod(),
                    responseObserver);
        }

        CompletableFuture<Void> failedFuture = new CompletableFuture<>();

        // create a shutdown actor that listens to exceptions.
        ActorRef shutdownActorRef = flatMapActorSystem.actorOf(FlatMapShutdownActor.props(failedFuture));

        // subscribe for dead letters
        flatMapActorSystem.getEventStream().subscribe(shutdownActorRef, AllDeadLetters.class);

        handleFailure(failedFuture, responseObserver);

        ActorRef flatMapActor = flatMapActorSystem
                .actorOf(FlatMapActor.props(
                        flatMapper,
                        shutdownActorRef,
                        responseObserver
                ));

        return new StreamObserver<FlatmapOuterClass.MapRequest>() {
            @Override
            public void onNext(FlatmapOuterClass.MapRequest mapRequest) {
                flatMapActor.tell(mapRequest, ActorRef.noSender());
            }

            @Override
            public void onError(Throwable throwable) {
                // We close the stream and let the sender retry the messages
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                // We close the stream as soon as we get the signal from the input. The inflight messages would error out but would be replayed back.
                responseObserver.onCompleted();
                flatMapActorSystem.stop(flatMapActor);
            }
        };
    }

    @Override
    public void isReady(
            Empty request,
            StreamObserver<FlatmapOuterClass.ReadyResponse> responseObserver) {
        responseObserver.onNext(FlatmapOuterClass.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }

}
