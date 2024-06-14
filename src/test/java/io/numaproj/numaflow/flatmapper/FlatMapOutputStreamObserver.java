package io.numaproj.numaflow.flatmapper;

import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.flatmap.v1.FlatmapOuterClass;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class FlatMapOutputStreamObserver implements StreamObserver<FlatmapOuterClass.MapResponse> {
    public AtomicReference<Boolean> completed = new AtomicReference<>(false);
    public AtomicReference<List<FlatmapOuterClass.MapResponse>> resultDatum = new AtomicReference<>(
            new ArrayList<>());
    public Throwable t;

    @Override
    public void onNext(FlatmapOuterClass.MapResponse response) {
        if (!response.getResult().getKeysList().isEmpty() && response
                .getResult()
                .getKeys(0)
                .equals("exception")) {
            throw new RuntimeException("unknown exception");
        }

        List<FlatmapOuterClass.MapResponse> receivedResponses = resultDatum.get();
        receivedResponses.add(response);
        resultDatum.set(receivedResponses);
    }

    @Override
    public void onError(Throwable throwable) {
        t = throwable;
    }

    @Override
    public void onCompleted() {
        this.completed.set(true);
    }
}
