package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** An explicitly empty native fixture, with its own bounded startup receipts. */
final class FreshNativeServer {
    private static final int MAX_LOG_BYTES = 1_048_576;
    private FreshNativeServer() { }

    record Request(String storeUri, String operatorStateDatabase, Path jar, List<String> applicationArguments) {
        Request {
            Objects.requireNonNull(storeUri); Objects.requireNonNull(operatorStateDatabase); Objects.requireNonNull(jar);
            applicationArguments = List.copyOf(applicationArguments);
        }
    }
    record Setup(Path jar, List<String> applicationArguments, Supplier<FreshMemberStartup.Freshness> freshness,
                 Function<Request, RealProcessServer> launch, Path receipts, LongSupplier clock) {
        Setup {
            Objects.requireNonNull(jar); applicationArguments = List.copyOf(applicationArguments);
            Objects.requireNonNull(freshness); Objects.requireNonNull(launch);
            Objects.requireNonNull(receipts); Objects.requireNonNull(clock);
        }
    }

    static RealProcessServer startFresh(String storeUri, String operatorStateDatabase) {
        Path jar = RealProcessServer.bootJar();
        try (StoreDocuments documents = StoreDocuments.at(storeUri)) {
            return startFresh(storeUri, operatorStateDatabase, new Setup(jar, List.of(), documents::freshMemberSetup,
                    request -> RealProcessServer.launchingWithJvmArguments(request.storeUri(),
                            request.operatorStateDatabase(), request.jar(), List.of(), request.applicationArguments()),
                    Path.of("target", "failure-scenes", "fresh-native"), System::nanoTime));
        }
    }

    static RealProcessServer startFresh(String storeUri, String operatorStateDatabase, Setup setup) {
        Request frozen = new Request(storeUri, operatorStateDatabase, setup.jar(), setup.applicationArguments());
        return FreshMemberStartup.start(setup.freshness(),
                () -> attempt(setup.launch().apply(frozen), setup.receipts()), setup.clock());
    }

    private static FreshMemberStartup.Attempt<RealProcessServer> attempt(RealProcessServer server, Path receipts) {
        return new FreshMemberStartup.Attempt<>() {
            private Snapshot snapshot;
            @Override public RealProcessServer await(Duration remaining) {
                server.awaitHealthy(remaining, true);
                return server;
            }
            @Override public FreshMemberStartup.Failure failure() {
                Snapshot captured = snapshot();
                return new FreshMemberStartup.Failure(server.baseUrl().getPort(), captured.ended(),
                        captured.exit(), new String(captured.bytes(), StandardCharsets.UTF_8));
            }
            @Override public void retain(int ordinal, FreshMemberStartup.Freshness fresh, Throwable cause) {
                Snapshot captured = snapshot();
                try {
                    Files.createDirectories(receipts);
                    Path directory = Files.createTempDirectory(receipts, "attempt-" + ordinal + "-");
                    Files.write(directory.resolve("server.raw.log"), captured.bytes(), StandardOpenOption.CREATE_NEW);
                    var metadata = new LinkedHashMap<String, Object>();
                    metadata.put("attempt", ordinal); metadata.put("pid", server.pid());
                    metadata.put("baseUri", server.baseUrl().toString()); metadata.put("httpPort", server.baseUrl().getPort());
                    metadata.put("argv", server.launchCommand()); metadata.put("sourceLog", server.output().toString());
                    metadata.put("capturedAt", captured.at()); metadata.put("ended", captured.ended());
                    if (captured.ended()) { metadata.put("exit", captured.exit()); }
                    metadata.put("rawLogState", captured.ended() ? "ENDED_CHILD_COMPLETE" : "LIVE_STARTUP_PREFIX");
                    metadata.put("rawLogBytes", captured.bytes().length); metadata.put("maximumLogBytes", MAX_LOG_BYTES);
                    metadata.put("noPipeline", fresh.noPipeline()); metadata.put("noDesired", fresh.noDesired());
                    metadata.put("noActual", fresh.noActual()); metadata.put("noWorkload", fresh.noWorkload());
                    metadata.put("freshnessConsistency", "NON_ATOMIC_EMPTY_PIPELINE_WORK_GUARDS");
                    metadata.put("conflictingPortOwner", "UNKNOWN");
                    if (cause != null) { metadata.put("failureType", cause.getClass().getName()); }
                    Files.writeString(directory.resolve("receipt.json"), JsonWriter.write(metadata), StandardOpenOption.CREATE_NEW);
                } catch (IOException unavailable) { throw new UncheckedIOException(unavailable); }
            }
            @Override public void close() {
                server.close();
                if (!server.terminated()) { throw new AssertionError("the failed fresh native child did not confirm termination"); }
            }
            private Snapshot snapshot() {
                if (snapshot != null) { return snapshot; }
                boolean endedBefore = server.terminated();
                try (InputStream input = Files.newInputStream(server.output())) {
                    byte[] bytes = input.readNBytes(MAX_LOG_BYTES + 1);
                    if (bytes.length > MAX_LOG_BYTES) { throw new AssertionError("the owned startup log exceeded its retained byte bound"); }
                    boolean ended = endedBefore && server.terminated();
                    snapshot = new Snapshot(bytes, ended, ended ? server.exitValue() : -1, Instant.now().toString());
                    return snapshot;
                } catch (IOException unavailable) { throw new UncheckedIOException(unavailable); }
            }
        };
    }
    private record Snapshot(byte[] bytes, boolean ended, int exit, String at) { }
}
