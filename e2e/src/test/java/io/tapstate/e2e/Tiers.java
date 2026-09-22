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
}
