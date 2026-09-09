package io.tapstate.adapters.pdk;

import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks, field by field, the identity a capture config carries and the namespace a connector's notes
 * are filed under because of it.
 *
 * <p>A connector finds its own notes only under the name they were written under. So an identity
 * dropped on the way through, or a namespace that derives differently than it did, reads at runtime as
 * an empty notepad — indistinguishable from a first run, and reported by nothing. Neither failure is a
 * compile error: a config can be handed on without its node, and the two ids can be joined another way,
 * with every signature unchanged. This golden is what makes such a change arrive as a diff to review
 * rather than as a connector that quietly starts over. Regenerate with
 * {@code -Dtapstate.identity.golden.update=true}, then review the diff.
 */
class ConnectorIdentityGoldenTest {

    private static final Path GOLDEN =
            Path.of("src", "test", "resources", "golden", "connector-identity.golden.json");
    private static final boolean UPDATE = Boolean.getBoolean("tapstate.identity.golden.update");

    /** One capture config per way a node can be named, in a fixed order. */
    private static List<Case> samples() {
        CaptureConfig pg = new CaptureConfig(
                "postgres", Map.of("uri", "postgres://host/db"), List.of("orders"));
        return List.of(
                new Case("a pipeline reading a source", pg.at(new PipelineNode("p-orders", "src_pg_main"))),
                new Case("a second pipeline on the same source", pg.at(new PipelineNode("p-audit", "src_pg_main"))),
                new Case("the write side of the same pipeline", pg.at(new PipelineNode("p-orders", "to_mongo"))),
                new Case("a node id carrying dots of its own", pg.at(new PipelineNode("p-orders", "src.a.b"))),
                new Case("a read-only drive naming no node", pg));
    }

    @Test
    void identityAndTheNamespaceItDerivesMatchTheCheckedInGolden() throws IOException {
        String rendered = render(samples());
        if (UPDATE) {
            Files.createDirectories(GOLDEN.getParent());
            Files.writeString(GOLDEN, rendered);
            return;
        }
        assertThat(Files.exists(GOLDEN))
                .as("identity golden missing — regenerate with -Dtapstate.identity.golden.update=true")
                .isTrue();
        assertThat(Files.readString(GOLDEN)).isEqualTo(rendered);
    }

    @Test
    void goldenUpdateToggleIsOffDuringNormalRuns() {
        // With the toggle set, the assertion path is skipped and the golden is rewritten — a real
        // identity regression would be silently rebaselined. This guard makes any toggled run RED.
        assertThat(UPDATE)
                .as("tapstate.identity.golden.update must not be set during a normal run — it rewrites the golden")
                .isFalse();
    }

    // ---- fixtures + a small deterministic JSON renderer (test-owned, stable) ----

    private record Case(String label, CaptureConfig config) {
    }

    private static String render(List<Case> cases) {
        StringBuilder sb = new StringBuilder("{\n");
        // Rendered alongside the per-node names because its distance from them is the contract: it sits
        // outside the per-node prefix so no sweep over that prefix can reach it.
        sb.append("  \"globalNamespace\": ").append(quote(ConnectorStateNamespace.GLOBAL)).append(",\n");
        sb.append("  \"cases\": [\n");
        for (int i = 0; i < cases.size(); i++) {
            Case c = cases.get(i);
            PipelineNode node = c.config().node();
            sb.append("    {\n");
            sb.append("      \"case\": ").append(quote(c.label())).append(",\n");
            sb.append("      \"connectorId\": ").append(quote(c.config().connectorId())).append(",\n");
            sb.append("      \"pipelineId\": ").append(node == null ? "null" : quote(node.pipelineId())).append(",\n");
            sb.append("      \"nodeId\": ").append(node == null ? "null" : quote(node.nodeId())).append(",\n");
            sb.append("      \"stateNamespace\": ").append(quote(ConnectorStateNamespace.of(node))).append("\n");
            sb.append(i + 1 < cases.size() ? "    },\n" : "    }\n");
        }
        return sb.append("  ]\n}\n").toString();
    }

    private static String quote(String s) {
        if (s == null) {
            return "null";
        }
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
