package io.tapstate.e2e;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Keeps already obtained return facts when a later diagnostic rejects the phase. */
final class BenchmarkReturnFailureRetention {
    private BenchmarkReturnFailureRetention() { }

    static <T> T run(Supplier<T> action, Supplier<Map<String, Object>> retainedEvidence,
            Consumer<Map<String, Object>> recorder) {
        try {
            return action.get();
        } catch (RuntimeException | Error failure) {
            try {
                var evidence = java.util.Objects.requireNonNull(retainedEvidence.get());
                if (!evidence.isEmpty()) {
                    var receipt = new java.util.LinkedHashMap<>(evidence);
                    receipt.put("failureType", failure.getClass().getName());
                    recorder.accept(Map.copyOf(receipt));
                }
            } catch (RuntimeException | Error recordingFailure) {
                if (recordingFailure != failure) { failure.addSuppressed(recordingFailure); }
            }
            throw failure;
        }
    }
}
