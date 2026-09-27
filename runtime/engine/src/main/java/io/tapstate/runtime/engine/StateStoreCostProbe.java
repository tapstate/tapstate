package io.tapstate.runtime.engine;

/**
 * Optional measurement hook at an operator's cold-layer boundary. Production leaves it unwired until
 * a job-scoped, cluster-complete projection exists. A benchmark may attach one without changing the
 * state path or serializing values a second time merely to measure them.
 */
public interface StateStoreCostProbe {

    enum Operation {
        LOAD, LOAD_ALL, SAVE, DELETE
    }

    enum Codec {
        ENCODE, DECODE
    }

    /** One successfully completed map-store call; payload bytes are the actual encoded arrays. */
    void completed(String namespace, Operation operation, long nanos, long payloadBytes);

    /** One map-store call that threw, including its blocked time but no claimed successful bytes. */
    void failed(String namespace, Operation operation, long nanos);

    /** One completed encode or decode of the supplied array. */
    void serialized(String namespace, Codec codec, long bytes);
}
