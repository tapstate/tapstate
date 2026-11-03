package io.tapstate.core.logging;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

class SecretRedactorTest {

    @Test
    void replacesRegisteredValuesAndTracksOwnerLifecycle() {
        SecretRedactor redactor = new SecretRedactor();
        redactor.replace("source-a", List.of("sentinel", "sentinel-long"));
        redactor.replace("source-b", List.of("second-secret"));

        assertThat(redactor.redact("sentinel-long / sentinel / second-secret"))
                .isEqualTo("******** / ******** / ********");

        redactor.replace("source-a", List.of("replacement"));
        redactor.remove("source-b");

        assertThat(redactor.redact("sentinel second-secret replacement"))
                .isEqualTo("sentinel second-secret ********");
    }

    @Test
    void retainedOperationsKeepOriginalSecretsAfterSourceReplacementAndDeletion(){
        var original=new SecretRedactor();original.replace("source",List.of("old-secret"));original.replace("unrelated",List.of("other-secret"));var retained=original.fork(List.of("source"));
        original.replace("source",List.of("new-secret"));original.remove("source");
        assertThat(retained.redact("old-secret")).isEqualTo("********");
        assertThat(retained.redact("other-secret")).isEqualTo("other-secret");
        assertThat(original.redact("old-secret")).isEqualTo("old-secret");
        retained.replace("retained-inputs",List.of("saved-secret"));
        assertThat(original.redact("saved-secret")).isEqualTo("saved-secret");
    }

    @Test
    void ignoresEmptyValuesAndLeavesNullTextNull() {
        SecretRedactor redactor = new SecretRedactor();
        redactor.replace("source", List.of("", "  "));

        assertThat(redactor.redact("ordinary line")).isEqualTo("ordinary line");
        assertThat(redactor.redact(null)).isNull();
    }

    @Test
    void redactsShortCredentialsAsTokensAndLeavesDiagnosticsAndMarkersIntact() {
        SecretRedactor redactor = new SecretRedactor();
        redactor.replace("source", List.of("ed", "pass"));

        assertThat(redactor.redact("authentication <redacted>; password; ed; passphrase; pass"))
                .isEqualTo("authentication <redacted>; password; ********; passphrase; ********");
    }

    @Test
    void repeatedRedactionDoesNotRewriteExistingMarkers() {
        SecretRedactor redactor = new SecretRedactor();
        redactor.replace("source", List.of("ed"));

        String once = redactor.redact("private key ed");

        assertThat(redactor.redact(once)).isEqualTo(once);
        assertThat(redactor.redact("already <redacted>")).isEqualTo("already <redacted>");
    }

    @Test
    void publishesCompleteSnapshotsToConcurrentReaders() throws Exception {
        SecretRedactor redactor = new SecretRedactor();
        CountDownLatch start = new CountDownLatch(1);

        try (ExecutorService executor = Executors.newFixedThreadPool(4)) {
            List<Future<?>> tasks = new ArrayList<>();
            tasks.add(executor.submit(() -> {
                await(start);
                for (int iteration = 0; iteration < 2_000; iteration++) {
                    redactor.replace("source", List.of(iteration % 2 == 0 ? "alpha" : "beta"));
                    if (iteration % 3 == 0) {
                        redactor.remove("source");
                    }
                }
            }));
            for (int reader = 0; reader < 3; reader++) {
                tasks.add(executor.submit(() -> {
                    await(start);
                    for (int iteration = 0; iteration < 2_000; iteration++) {
                        assertThat(redactor.redact("alpha beta"))
                                .isIn("******** beta", "alpha ********", "alpha beta");
                    }
                }));
            }

            start.countDown();
            for (Future<?> task : tasks) {
                task.get();
            }
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }
}
