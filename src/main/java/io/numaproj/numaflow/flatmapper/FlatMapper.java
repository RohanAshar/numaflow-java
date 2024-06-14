package io.numaproj.numaflow.flatmapper;

/**
 * FlatMapper exposes method for performing bi-directional map streaming operation.
 * Implementations should override the processMessage method
 * which will be used for processing the input messages
 */

public abstract class FlatMapper {
    /**
     * method which will be used for processing streaming messages.
     *
     * @param keys message keys
     * @param datum current message to be processed
     * @return MessageList which contains output from map
     */
    public abstract MessageList processMessage(String[] keys, Datum datum);
}
