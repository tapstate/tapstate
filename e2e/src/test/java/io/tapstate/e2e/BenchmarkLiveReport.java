package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Atomically persists every completed benchmark fork and the final outcome. */
final class BenchmarkLiveReport {

    private final Path output;
    private final Map<String, Object> document = new LinkedHashMap<>();
    private final List<Object> forks = new ArrayList<>();

    BenchmarkLiveReport(Path output) {
        if (output == null || !output.isAbsolute() || output.getFileName() == null) {
            throw new IllegalArgumentException("benchmark output must be an absolute file path");
        }
        this.output = output.toAbsolutePath().normalize();
        if (Files.exists(this.output)) {
            throw new IllegalArgumentException("benchmark output already exists: " + this.output);
        }
        document.put("formatVersion", 1);
        document.put("status", "INITIALIZING");
        document.put("startedAt", Instant.now().toString());
        document.put("forks", forks);
        persist();
    }

    void begin(Map<String, Object> inputs, Map<String, Object> environment,
            List<Map<String, Object>> workloads) {
        document.put("inputs", inputs);
        document.put("environment", environment);
        document.put("workloads", workloads);
        document.put("status", "RUNNING");
        persist();
    }

    void addFork(Map<String, Object> fork) {
        forks.add(new LinkedHashMap<>(fork));
        document.put("lastCompletedForkAt", Instant.now().toString());
        persist();
    }

    void finish(Map<String, Object> evaluation, boolean passed) {
        document.put("evaluation", evaluation);
        document.put("status", passed ? "PASSED" : "FAILED");
        document.put("finishedAt", Instant.now().toString());
        persist();
    }

    void fail(Throwable failure) {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("type", failure.getClass().getName());
        error.put("message", String.valueOf(failure.getMessage()));
        error.put("topStackFrames", java.util.Arrays.stream(failure.getStackTrace()).limit(12)
                .map(StackTraceElement::toString).toList());
        document.put("failure", error);
        document.put("status", "FAILED");
        document.put("finishedAt", Instant.now().toString());
        persist();
    }

    Path output() {
        return output;
    }

    private void persist() {
        Path parent = output.getParent();
        Path temporary = null;
        UncheckedIOException writeFailure = null;
        try {
            Files.createDirectories(parent);
            temporary = Files.createTempFile(parent, output.getFileName().toString() + ".", ".tmp");
            byte[] bytes = (JsonWriter.write(document) + "\n").getBytes(StandardCharsets.UTF_8);
            try (FileChannel channel = FileChannel.open(temporary,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            Files.move(temporary, output, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException failure) {
            writeFailure = new UncheckedIOException("cannot persist benchmark evidence at " + output, failure);
            throw writeFailure;
        } finally {
            if (temporary != null) {
                try {
                    Files.deleteIfExists(temporary);
                } catch (IOException cleanupFailure) {
                    if (writeFailure != null) {
                        writeFailure.addSuppressed(cleanupFailure);
                    } else {
                        throw new UncheckedIOException("cannot remove benchmark temporary file " + temporary,
                                cleanupFailure);
                    }
                }
            }
        }
    }
}
