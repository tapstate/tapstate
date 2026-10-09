package io.tapstate.e2e;

import java.util.function.BiFunction;

/**
 * The fidelity axis: the same specification run against the product embedded in this JVM and against
 * the shipped boot jar in its own process. It is the only thing that differs between two runs of one
 * example, and the axis a real-connector witness sweeps so the shipped deliverable itself is
 * exercised - a connector loaded by the fat-jar it ships in - not only a server the test embeds.
 */
enum Tiers {

    IN_PROCESS(InProcessServer::start),
    REAL_PROCESS(RealProcessServer::start);

    private final BiFunction<String, String, ServerHandle> launcher;

    Tiers(BiFunction<String, String, ServerHandle> launcher) {
        this.launcher = launcher;
    }

    ServerHandle launch(String storeUri) {
        return launch(storeUri, SharedMongo.OPERATOR_STATE_DATABASE);
    }

    ServerHandle launch(String storeUri, String operatorStateDatabase) {
        return launcher.apply(storeUri, operatorStateDatabase);
    }
    /** Explicitly starts an empty fixture; ordinary launch and recovery retain their existing behavior. */
    ServerHandle launchFresh(String storeUri) {
        return launchFresh(storeUri, SharedMongo.OPERATOR_STATE_DATABASE);
    }

    ServerHandle launchFresh(String storeUri, String operatorStateDatabase) {
        return this == IN_PROCESS ? launch(storeUri, operatorStateDatabase)
                : FreshNativeServer.startFresh(storeUri, operatorStateDatabase);
    }

    /** Per-call native boundaries for routing controls; no process-wide launcher override. */
    ServerHandle launchFresh(String storeUri, String operatorStateDatabase, FreshNativeServer.Setup setup) {
        return this == IN_PROCESS ? launch(storeUri, operatorStateDatabase)
                : FreshNativeServer.startFresh(storeUri, operatorStateDatabase, setup);
    }
}
