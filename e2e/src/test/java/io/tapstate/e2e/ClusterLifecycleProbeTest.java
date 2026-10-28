package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapstate.e2e.ClusterLifecycleProbe.Phase.PREPARED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The observer restores scope, rejects foreign close and cannot substitute its failure for an action. */
class ClusterLifecycleProbeTest {
    @Test
    void nestedScopesRestoreTheirExactPreviousListenerAndNeverExposeRawParameters(@TempDir Path scratch) {
        List<ClusterLifecycleProbe.Event> outer = new ArrayList<>();
        List<ClusterLifecycleProbe.Event> inner = new ArrayList<>();
        String sensitive = "probe-sensitive-parameter-sentinel";
        try (ClusterLifecycleProbe.Scope first = ClusterLifecycleProbe.install(outer::add)) {
            ClusterLifecycleProbe.Facts firstFacts = facts(scratch, sensitive);
            try (ClusterLifecycleProbe.Scope second = ClusterLifecycleProbe.install(inner::add)) {
                assertThatThrownBy(first::close).isInstanceOf(IllegalStateException.class);
                ClusterLifecycleProbe.record(PREPARED, facts(scratch, sensitive), -1);
            }
            ClusterLifecycleProbe.record(PREPARED, firstFacts, -1);
        }
        assertThat(outer).hasSize(1);
        assertThat(inner).hasSize(1);
        assertThat(outer.toString()).doesNotContain(sensitive, scratch.toString(), "mongodb://");
        assertThat(inner.toString()).doesNotContain(sensitive, scratch.toString(), "mongodb://");
        assertThat(facts(scratch, sensitive)).isNull();
    }

    @Test
    void anObserverFailureIsReportedAfterTheObservedActionAndRestoration(@TempDir Path scratch) throws Exception {
        RuntimeException intended = new IllegalStateException("observer failed");
        Path actualAction = scratch.resolve("observed-action");
        try (ClusterLifecycleProbe.Scope scope = ClusterLifecycleProbe.install(ignored -> { throw intended; })) {
            assertThatCode(() -> {
                ClusterLifecycleProbe.record(PREPARED, facts(scratch, "input"), -1);
                Files.writeString(actualAction, "the action ran");
            }).doesNotThrowAnyException();
            assertThat(Files.readString(actualAction)).isEqualTo("the action ran");
            assertThatThrownBy(scope::close).isSameAs(intended);
        }
        assertThat(facts(scratch, "input")).isNull();
    }

    @Test
    void aForeignThreadCannotCloseTheInstallingThreadsScope(@TempDir Path scratch) throws InterruptedException {
        AtomicReference<Throwable> rejected = new AtomicReference<>();
        try (ClusterLifecycleProbe.Scope scope = ClusterLifecycleProbe.install(ignored -> { })) {
            Thread foreign = new Thread(() -> {
                try { scope.close(); }
                catch (RuntimeException | Error failure) { rejected.set(failure); }
            });
            foreign.start();
            assertThat(foreign.join(java.time.Duration.ofSeconds(5))).isTrue();
            assertThat(rejected.get()).isInstanceOf(IllegalStateException.class);
            assertThat(facts(scratch, "input")).isNotNull();
        }
        assertThat(facts(scratch, "input")).isNull();
    }

    private static ClusterLifecycleProbe.Facts facts(Path scratch, String sensitive) {
        return ClusterLifecycleProbe.facts(28123, scratch, scratch.resolve("out"), scratch.resolve("stage"),
                "mongodb://" + sensitive, "operator", "127.0.0.1", List.of(),
                List.of("--tapstate.cluster.id=control-cluster", "--tapstate.cluster.node-id=node-a",
                        "--tapstate.hz.member-port=28124"));
    }
}
