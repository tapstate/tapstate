package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A malformed observation cannot abort an action or expose its raw parameter through the probe. */
class ClusterProbeLinkMetadataTest {
    @Test
    void aLinkPortParseFailureIsDeferredUntilScopeCloseAndDoesNotReplaceTheAction(@TempDir Path scratch)
            throws Exception {
        String raw = "link-parameter-sensitive-sentinel";
        Path action = scratch.resolve("action-ran");
        try (ClusterLifecycleProbe.Scope scope = ClusterLifecycleProbe.install(ignored -> { })) {
            assertThatCode(() -> {
                ClusterLifecycleProbe.Facts facts = ClusterLifecycleProbe.facts(28123, scratch,
                        scratch.resolve("out"), scratch.resolve("stage"), "mongodb://input", "operator",
                        "127.0.0.1", List.of(), List.of("--tapstate.cluster.id=control",
                                "--tapstate.cluster.node-id=node-a", "--tapstate.hz.member-port=28124",
                                "--tapstate.hz.advertised-member-address=127.0.0.1:" + raw));
                assertThat(facts).isNull();
                Files.writeString(action, "ran after observation failure");
            }).doesNotThrowAnyException();
            assertThat(Files.readString(action)).isEqualTo("ran after observation failure");
            assertThatThrownBy(scope::close).isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("the fixture link port is not observable")
                    .satisfies(failure -> {
                        assertThat(failure.getCause()).isNull();
                        assertThat(failure.toString()).doesNotContain(raw, "mongodb://", scratch.toString());
                    });
        }
    }
}
