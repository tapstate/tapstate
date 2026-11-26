package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class OversizedConnectorUploadKeepsItsRefusalIT {

    @Test
    void cleanupFailureDoesNotReplaceTheOversizedUploadRefusal(@TempDir Path work) throws Exception {
        DockerGate.require();
        Path plugins = Files.createDirectory(work.resolve("plugins"));
        Path remainder = work.resolve("upload.bin");
        try (var file = new RandomAccessFile(remainder.toFile(), "rw")) {
            file.setLength(256L * 1024 * 1024);
        }

        try (var storage = PreparedNativeStore.create();
                var server = InProcessServer.startPrepared(storage,
                        List.of("--" + ServerHandle.PLUGINS_DIRECTORY_SETTING + "=" + plugins));
                var client = HttpClient.newHttpClient()) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            var resumeUpload = new CountDownLatch(1);
            var firstChunk = HttpRequest.BodyPublishers.ofByteArray(new byte[64 * 1024]);
            var rest = HttpRequest.BodyPublishers.ofInputStream(() -> {
                try {
                    if (!resumeUpload.await(45, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Staging file was not made undeletable");
                    }
                    return Files.newInputStream(remainder);
                } catch (IOException failure) {
                    throw new UncheckedIOException(failure);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Upload was interrupted", interrupted);
                }
            });
            var request = HttpRequest.newBuilder(server.baseUrl().resolve("/api/connector-center:upload"))
                    .timeout(Duration.ofSeconds(60))
                    .header("Authorization", "Bearer " + control.credential())
                    .header("Content-Type", "application/java-archive")
                    .POST(HttpRequest.BodyPublishers.concat(firstChunk, rest))
                    .build();
            var upload = client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
            try {
                Path staged = Await.answered("the first upload chunk to reach the staging file",
                        Duration.ofSeconds(30), () -> writtenStagingFile(plugins, upload));
                // The server retains the open file. A nonempty directory at the old path makes unlink fail.
                Files.move(staged, work.resolve("open-upload.bin"));
                Files.createDirectory(staged);
                Files.createFile(staged.resolve("keep"));
                resumeUpload.countDown();

                var response = upload.get(60, TimeUnit.SECONDS);
                assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
                Map<?, ?> operation = (Map<?, ?>) JsonReader.parse(response.body());
                assertThat(operation.get("state")).isEqualTo("FAILED");
                assertThat(operation.get("errorCode")).as(response.body()).isEqualTo("TS0060013");
                Map<?, ?> failure = (Map<?, ?>) operation.get("failure");
                assertThat(failure.get("code")).isEqualTo("TS0060013");
                assertThat(((Map<?, ?>) failure.get("params")).get("artifact")).isEqualTo("JAR");
                assertThat(response.body()).doesNotContain("DirectoryNotEmptyException", plugins.toString());
            } finally {
                upload.cancel(true);
                resumeUpload.countDown();
            }
        }
    }

    private static Optional<Path> writtenStagingFile(Path plugins,
            CompletableFuture<HttpResponse<String>> upload) {
        if (upload.isDone()) {
            throw new AssertionError("Upload completed before writing the staging file: " + upload.join().body());
        }
        try (var files = Files.list(plugins)) {
            for (Path path : files.toList()) {
                if (path.getFileName().toString().startsWith(".connector-") && Files.size(path) > 0) {
                    return Optional.of(path);
                }
            }
            return Optional.empty();
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }
}
