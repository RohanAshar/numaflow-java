package io.numaproj.numaflow.flatmapper;

import io.numaproj.numaflow.mapstreamer.Datum;
import io.numaproj.numaflow.mapstreamer.OutputObserver;

/**
 * MapStreamer exposes method for performing map streaming operation.
 * Implementations should override the processMessage method
 * which will be used for processing the input messages
 */

public abstract class FlatMap {
    /**
     * method which will be used for processing streaming messages.
     *
     * @param keys message keys
     * @param datum current message to be processed
     * @param outputObserver observer of the response
     */
    public abstract void processMessage(String[] keys, Datum datum, OutputObserver outputObserver);
}
