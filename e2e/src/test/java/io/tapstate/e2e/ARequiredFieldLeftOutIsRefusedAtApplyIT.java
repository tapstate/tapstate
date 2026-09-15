package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Leaving a required field out of an artifact comes back from the running product as a coded
 * refusal naming the field, rather than as a server fault naming nothing.
 *
 * <p>The unit cases prove the parser refuses the document it is handed. What they cannot prove is
 * what an author actually receives: the parse runs inside an apply, behind an HTTP surface that
 * turns a coded refusal into an answer and an unexpected crash into a fault carrying no code and no
 * field. Those two outcomes are indistinguishable from inside the parser and are the whole
 * difference from outside it, so the boundary is where this is settled.
 *
 * <p>Both halves are asserted. That the apply was refused is satisfied by any failure at all,
 * including a server that would not boot; the code and the field path together are what say the
 * product diagnosed the author's document. The listing afterwards covers the other way this can go
 * wrong: a refusal that had already filed part of the batch leaves a workspace to unpick by hand.
 */
class ARequiredFieldLeftOutIsRefusedAtApplyIT {

    private static final String MISSING_FIELD = "dsl.missing-field";

    /** Complete but for the one field the join type requires; everything else is well formed. */
    private static final String JOIN_WITHOUT_SQL = """
            version: tapstate/v1
            kind: transform
            id: orders_wide
            type: join
            engine: builtin
            """;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aJoinWithoutItsSqlIsRefusedByTheFieldItLeftOutAndNoneOfTheBatchIsFiled() {
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_required_field"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");

            ControlPlane.Refusal refusal =
                    control.applyExpectingRefusal(Map.of("orders_wide.tap.yml", JOIN_WITHOUT_SQL));

            assertThat(refusal.code())
                    .as("the code refusing a document that left out a field its type requires")
                    .isEqualTo(MISSING_FIELD);
            assertThat(refusal.params())
                    .as("the field path is what sends the author to the line they have to edit")
                    .containsEntry("path", "sql");
            assertThat(control.artifactIds())
                    .as("what the server holds after refusing the batch")
                    .doesNotContain("orders_wide");
        }
    }
}
