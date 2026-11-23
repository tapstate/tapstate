package io.tapstate.adapters.pdk;

/** Keeps a real PDK logger handle after its opening Pipeline retires. */
public final class PipelineLogEmitterFixture {
    private PipelineLogEmitterFixture() {}

    public static java.util.function.Consumer<String> boundTo(String id, long generation) {
        var original = new ConnectorLog("csv", id, generation);
        return message -> original.warn(message);
    }
}
