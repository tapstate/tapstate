package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.UncheckedIOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.tapstate.e2e.ClusterProcessControlFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Files and real socket ownership distinguish preparation, cancellation and process hand-off. */
class RealProcessServerPreparationTest {
    @Test
    void allResourcesAreAllocatedBeforeAnyLaunchAndHandedOffDescriptorsDoNotStopTheirChildren(
            @TempDir Path scratch) throws Exception {
        List<RealProcessServer.PreparedLaunch> prepared = new ArrayList<>();
        List<RealProcessServer> children = new ArrayList<>();
        try {
            for (int member = 0; member < 3; member++) {
                Path ready = scratch.resolve("member-" + member + ".ready");
                prepared.add(prepare(jar(scratch), null,
                        ignored -> arguments(ready, "quiet", null, null, null)));
            }
            assertThat(prepared.stream().map(RealProcessServer.PreparedLaunch::httpPort).toList())
                    .doesNotHaveDuplicates();
            assertThat(prepared.stream().map(RealProcessServer.PreparedLaunch::workingDirectory).toList())
                    .doesNotHaveDuplicates();
            assertThat(prepared.stream().map(RealProcessServer.PreparedLaunch::stagingDirectory).toList())
                    .doesNotHaveDuplicates();
            for (RealProcessServer.PreparedLaunch launch : prepared) {
                assertThat(launch.workingDirectory()).isDirectory();
                assertThat(launch.output()).isRegularFile();
                assertThat(Files.size(launch.output())).isZero();
                assertThat(launch.stagingDirectory()).isDirectory();
                // This child never binds HTTP. An early launch releases this reservation, so the
                // real bind below catches it without relying on a child scheduler or a cached flag.
                assertThatThrownBy(() -> {
                    try (ServerSocket unexpected = takingPort(launch.httpPort())) {
                        // A successful bind is the regression; close it before AssertJ reports it.
                    }
                }).isInstanceOf(BindException.class);
            }
            for (RealProcessServer.PreparedLaunch launch : prepared) children.add(launch.launch());
            for (int member = 0; member < children.size(); member++) {
                RealProcessServer child = children.get(member);
                Path ready = scratch.resolve("member-" + member + ".ready");
                Await.until("the prepared child to acknowledge its PID", Duration.ofSeconds(5),
                        () -> acknowledgesPid(ready, child.pid()), child::tail);
                assertThat(Files.readString(ready)).isEqualTo(Long.toString(child.pid()));
                prepared.get(member).close();
                assertThat(child.isAlive()).isTrue();
                assertThat(child.stagingDirectory()).isDirectory();
            }
        } finally {
            try { retire(children); }
            finally { for (RealProcessServer.PreparedLaunch launch : prepared) launch.close(); }
        }
        for (RealProcessServer.PreparedLaunch launch : prepared) {
            assertThat(launch.stagingDirectory()).doesNotExist();
            assertThat(launch.output()).isRegularFile();
            assertThat(launch.workingDirectory()).isDirectory();
        }
    }

    @Test
    void aReadyFileMustContainTheCompleteOwnedPidBeforeTheWaitCanFinish(@TempDir Path scratch) throws Exception {
        Path childReady = scratch.resolve("child.ready");
        try (RealProcessServer.PreparedLaunch prepared = prepare(jar(scratch), null,
                ignored -> arguments(childReady, "quiet", null, null, null));
             RealProcessServer child = prepared.launch()) {
            Path controlled = scratch.resolve("controlled.ready");
            String pid = Long.toString(child.pid());
            String partial = pid.substring(0, pid.length() - 1);
            assertThat(acknowledgesPid(controlled, child.pid())).isFalse();
            Files.createFile(controlled);
            assertThat(acknowledgesPid(controlled, child.pid())).as("creation is not acknowledgement").isFalse();
            for (String incomplete : List.of(partial, pid + "0", pid + "\n")) {
                Files.writeString(controlled, incomplete);
                assertThat(acknowledgesPid(controlled, child.pid())).isFalse();
            }
            Files.writeString(controlled, "");
            CountDownLatch emptyRead = new CountDownLatch(1);
            CountDownLatch partialRead = new CountDownLatch(1);
            try (var observer = Executors.newSingleThreadExecutor()) {
                var waiting = observer.submit(() -> Await.until("a complete controlled child PID", Duration.ofSeconds(5), () -> {
                    boolean ready = acknowledgesPid(controlled, child.pid());
                    if (!ready) {
                        try {
                            String observed = Files.readString(controlled);
                            if (observed.isEmpty()) emptyRead.countDown();
                            if (observed.equals(partial)) partialRead.countDown();
                        } catch (java.io.IOException failure) {
                            throw new UncheckedIOException(failure);
                        }
                    }
                    return ready;
                }, child::tail));
                try {
                    assertThat(emptyRead.await(5, TimeUnit.SECONDS)).isTrue();
                    assertThat(waiting.isDone()).isFalse();
                    Files.writeString(controlled, partial);
                    assertThat(partialRead.await(5, TimeUnit.SECONDS)).isTrue();
                    assertThat(waiting.isDone()).isFalse();
                    Files.writeString(controlled, pid);
                    waiting.get(5, TimeUnit.SECONDS);
                    assertThat(child.isAlive()).isTrue();
                } finally {
                    waiting.cancel(true);
                }
            }
        }
    }

    @Test
    void cancellingUnlaunchedResourcesKeepsNamedStagingAndDiagnostics(@TempDir Path scratch) throws Exception {
        Path named = Files.createDirectory(scratch.resolve("named"));
        Path sentinel = Files.writeString(named.resolve("sentinel"), "owned by the witness");
        RealProcessServer.PreparedLaunch owned = prepare(jar(scratch), null, ignored -> List.of());
        RealProcessServer.PreparedLaunch supplied = prepare(jar(scratch), named, ignored -> List.of());
        owned.close();
        supplied.close();
        owned.close();
        supplied.close();
        assertThat(owned.stagingDirectory()).doesNotExist();
        assertThat(Files.readString(sentinel)).isEqualTo("owned by the witness");
        assertThat(owned.output()).isRegularFile();
        assertThat(supplied.output()).isRegularFile();
        assertThat(Files.size(owned.output())).isZero();
        assertThat(Files.size(supplied.output())).isZero();
        assertThat(owned.workingDirectory()).isDirectory();
        assertThat(supplied.workingDirectory()).isDirectory();
        try (ServerSocket first = takingPort(owned.httpPort()); ServerSocket second = takingPort(supplied.httpPort())) {
            assertThat(first.getLocalPort()).isNotEqualTo(second.getLocalPort());
        }
        assertThatThrownBy(owned::launch).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(supplied::launch).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aSecondPreparationFailureReleasesItsReservationAndKeepsCallerOwnedStaging(@TempDir Path scratch)
            throws Exception {
        Path named = Files.createDirectory(scratch.resolve("second-named"));
        Path sentinel = Files.writeString(named.resolve("sentinel"), "retained");
        RuntimeException intended = new IllegalStateException("second preparation failed");
        AtomicInteger failingPort = new AtomicInteger();
        try (RealProcessServer.PreparedLaunch first = prepare(jar(scratch), null, ignored -> List.of())) {
            assertThatThrownBy(() -> prepare(jar(scratch), named, port -> {
                failingPort.set(port);
                throw intended;
            })).isSameAs(intended);
            assertThat(first.stagingDirectory()).isDirectory();
            try (ServerSocket reused = takingPort(failingPort.get())) {
                assertThat(reused.getLocalPort()).isEqualTo(failingPort.get());
            }
            assertThat(Files.readString(sentinel)).isEqualTo("retained");
        }
    }

    @Test
    void aLaunchFailureCancelsOnlyItsUnhandedResources(@TempDir Path scratch) throws Exception {
        try (RealProcessServer.PreparedLaunch launch = prepare(jar(scratch), null, ignored -> List.of())) {
            Files.delete(launch.output());
            Files.delete(launch.workingDirectory());
            Files.writeString(launch.workingDirectory(), "diagnostic path made unusable by this control");
            assertThatThrownBy(launch::launch).isInstanceOf(UncheckedIOException.class);
            assertThat(launch.stagingDirectory()).doesNotExist();
            assertThat(Files.readString(launch.workingDirectory()))
                    .isEqualTo("diagnostic path made unusable by this control");
            try (ServerSocket reused = takingPort(launch.httpPort())) {
                assertThat(reused.getLocalPort()).isEqualTo(launch.httpPort());
            }
        }
    }

    @Test
    void omittingTheOriginalHealthBarrierCannotHideAChildThatExited(@TempDir Path scratch) throws Exception {
        try (RealProcessServer child = child(scratch, "exit", "exit-seven", null, null, null)) {
            Await.until("the controlled non-serving child to exit", Duration.ofSeconds(5),
                    () -> !child.isAlive(), child::tail);
            assertThatThrownBy(child::awaitHealthy).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("exited with status 7");
        }
    }
}
