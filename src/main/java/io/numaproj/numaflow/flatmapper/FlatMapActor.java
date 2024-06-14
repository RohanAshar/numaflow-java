package io.numaproj.numaflow.flatmapper;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.flatmap.v1.FlatmapOuterClass;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
//@AllArgsConstructor
public class FlatMapActor extends AbstractActor {
    private final FlatMapper flatMapper;
    private final ActorRef shutdownActor;
    private final StreamObserver<FlatmapOuterClass.MapResponse> responseObserver;

    public FlatMapActor(
            FlatMapper flatMapper,
            ActorRef shutdownActor,
            StreamObserver<FlatmapOuterClass.MapResponse> responseObserver) {
        this.flatMapper = flatMapper;
        this.shutdownActor = shutdownActor;
        this.responseObserver = responseObserver;
    }

    public static Props props(
            FlatMapper flatMapHandler,
            ActorRef shutdownActor,
            StreamObserver<FlatmapOuterClass.MapResponse> responseObserver) {
        return Props.create(
                FlatMapActor.class,
                flatMapHandler,
                shutdownActor,
                responseObserver);
    }

    @Override
    public void preRestart(Throwable reason, Optional<Object> message) {
        log.debug("supervisor pre restart was executed");
        shutdownActor.tell(reason, ActorRef.noSender());
        Service.flatMapActorSystem.stop(getSelf());
    }

    @Override
    public void postStop() {
        log.debug("post stop of supervisor executed - {}", getSelf().toString());
        shutdownActor.tell(Constants.SUCCESS, ActorRef.noSender());
    }


    @Override
    public Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(FlatmapOuterClass.MapRequest.class, this::invokeHandler)
                .build();
    }

    private void invokeHandler(FlatmapOuterClass.MapRequest mapRequest) {
        HandlerDatum handlerDatum = new HandlerDatum(
                mapRequest.getValue().toByteArray(),
                Instant.ofEpochSecond(
                        mapRequest.getWatermark().getSeconds(),
                        mapRequest.getWatermark().getNanos()),
                Instant.ofEpochSecond(
                        mapRequest.getEventTime().getSeconds(),
                        mapRequest.getEventTime().getNanos()),
                mapRequest.getHeadersMap()
        );
        MessageList resultMessages = this.flatMapper.processMessage(mapRequest.getKeysList().toArray(new String[0]), handlerDatum);
        resultMessages.getMessages().forEach(message -> {
            // Stream the response back to the sender
            responseObserver.onNext(buildActorResponse(message, mapRequest.getUuid()));
        });
        // Send an EOR message after the batch is completed to indicate the request is completely served
        responseObserver.onNext(buildEORResponse(mapRequest.getUuid()));
    }

    /***
     * Builds a response object from the message returned from UDF
     *
     * @param message message from the UDF
     * @param uuid uuid sent in the request
     * @return
     */
    private FlatmapOuterClass.MapResponse buildActorResponse(Message message, String uuid) {
        return FlatmapOuterClass.MapResponse.newBuilder()
                .setResult(FlatmapOuterClass.MapResponse.Result.newBuilder()
                        .setValue(
                                message.getValue() == null ? ByteString.EMPTY : ByteString.copyFrom(
                                        message.getValue()))
                        .addAllKeys(message.getKeys()
                                == null ? Collections.emptyList() : List.of(message.getKeys()))
                        .addAllTags(message.getTags()
                                == null ? Collections.emptyList() : List.of(message.getTags()))
                        .setEOR(false)
                        .setUuid(uuid)
                        .build())
                .build();

    }

    /***
     * Returns an EOR response message.
     *
     * @param uuid uuid sent in the request
     * @return
     */
    private FlatmapOuterClass.MapResponse buildEORResponse(String uuid) {
        return FlatmapOuterClass.MapResponse.newBuilder()
                .setResult(FlatmapOuterClass.MapResponse.Result.newBuilder()
                        .setEOR(true)
                        .setUuid(uuid)
                        .build())
                .build();

    }
}
