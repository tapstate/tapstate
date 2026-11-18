package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.StorageBinding;
import java.time.Duration;
import java.util.Map;

/** A separate reader process witnesses the artifact actually selected by the compatibility test. */
public final class RuntimeObservationReaderProcess {
    private RuntimeObservationReaderProcess() {}

    public static void main(String[] args) {
        if (args.length == 1 && args[0].equals("verify-reader")) {
            var projection = Map.<String, Object>of("message", "safe cause");
            var stored = MongoRuntimeDiagnostic.document(projection);
            if (!MongoRuntimeDiagnostic.read(stored).equals(projection)) throw new AssertionError("reader unavailable");
            System.out.println("COMPATIBLE_RUNTIME_READER_READY");
            return;
        }
        var connection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, args[0], args[1], null);
        try (var domain = MongoCacheDomain.connect(connection, Duration.ofSeconds(5), "runtime-recovery-reader")) {
            try {
                var scope = new PipelineIntentScope("cluster-a", 1, "incarnation-a", "orders", 1);
                var failure = domain.pipelineObservations(scope).read("orders").orElseThrow().failure();
                if (args[2].equals("unreadable")) throw new AssertionError("old reader unexpectedly accepted extension");
                if (!failure.code().equals("TS0060027") || !failure.params().equals(Map.of("connector", "fixture", "detail", "refused"))
                        || !failure.diagnostic().equals(Map.of("message", "safe cause"))) {
                    throw new AssertionError("original failure or attachment changed");
                }
                System.out.println("RUNTIME_DIAGNOSTIC_RECOVERED");
            } catch (TapstateException refusal) {
                if (!args[2].equals("unreadable") || refusal.code() != IoError.DOCUMENT_UNREADABLE) throw refusal;
                System.out.println("OLD_RUNTIME_READER_REFUSED_EXTENSION");
            }
        }
    }
}
