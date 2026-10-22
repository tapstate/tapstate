package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The witness that runtime registration refuses a connector this release does not support, cleanly and
 * without keeping any of it.
 *
 * <p>The artifact is the harness's own connector packaged under an id nothing supports. It is well
 * formed in every other way - the same classes, manifest and specification shape the harness registers
 * successfully elsewhere - so the only reason to refuse it is the id, and a refusal here cannot be a
 * packaging accident wearing the right error code.
 *
 * <p>Two assertions, because either alone is satisfied by an outcome this test exists to exclude.
 * Asserting only the code leaves "refused, having already filed the bytes and half-loaded the
 * connector" indistinguishable from a clean refusal; asserting only the absence leaves a bare crash
 * that stored nothing looking like a pass. The status is part of the first: a coded refusal answers a
 * request error, while an uncaught failure would answer a server error with no code at all.
 *
 * <p>Both fidelity tiers. The accepted set is widenable per deployment and the harness widens it - to
 * its own synthetic id and nothing else - so this also pins that the widening stays narrow in the
 * shipped boot jar, where it arrives as a command-line argument rather than a property set in this JVM.
 *
 * <p>Needs Docker for the store, and nothing else: no connector jars are staged for it, so unlike the
 * real-connector witnesses this one runs in an ordinary build.
 */
class UnofficialConnectorIsRefusedIT {

    /** An id no release supports, and one the harness does not widen the accepted set to. */
    private static final String UNOFFICIAL_ID = "e2e_unofficial";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void registeringAConnectorOutsideTheOfficialSetIsRefusedAndNothingIsFiledUnderIt(
            Tiers tier, @TempDir Path directory) throws Exception {
        byte[] artifact = Files.readAllBytes(E2eConnectorJar.buildInto(directory, UNOFFICIAL_ID));

        try (var storage = PreparedNativeStore.create("e2e", "e2e-password"); ServerHandle server = tier.launch(storage)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);

            ControlPlane.Refusal refusal = control.registerConnectorExpectingRefusal(artifact);

            assertThat(refusal.code()).isEqualTo("connector.not-official");
            assertThat(refusal.status()).isEqualTo(400);
            assertThat(control.connectorIds()).doesNotContain(UNOFFICIAL_ID);
        }
    }

    /**
     * The boundary the supported set is drawn at: a managed variant of a supported engine is accepted,
     * an independent database product is not - however similar its wire protocol, its change capture is
     * its own mechanism and nothing here has been run against it.
     *
     * <p>These ids are the confusable ones, which is the whole reason they are named through the real
     * control plane rather than left to the unit that already sweeps the full list. {@code mariadb}
     * ends in a supported id, {@code tidb} speaks the same protocol as one, and {@code open-gauss} is a
     * fork of another. A guard implemented as a prefix or substring test refuses the harness's own
     * obviously-foreign id above while admitting all three of these, so this is the case that separates
     * "the set is checked" from "the set is a set".
     *
     * <p>One tier, unlike the case above. What differs per tier is where the widening arrives from, and
     * that is already pinned; what differs per id is the guard's own reading of the id, which does not
     * depend on how the server was launched. Sweeping both axes at once would multiply container runs
     * for no assertion neither one already makes.
     *
     * <p>No "nothing is filed under it" assertion here, and the reason is worth writing down because the
     * obvious version of it passes for the wrong reason. The catalog listing answers with registered
     * rows and bundled ones alike, and every id named here is bundled - the product knows these
     * connectors exist, it simply will not accept an artifact for one. So asserting the id is absent
     * from that listing fails against a correct server, and asserting it is present would witness
     * nothing. The clean-refusal half is carried by the case above, whose id is bundled nowhere.
     */
    @ParameterizedTest
    @ValueSource(strings = {"mariadb", "tidb", "open-gauss"})
    void anIndependentDatabaseProductIsRefusedByItsOwnId(String connectorId, @TempDir Path directory)
            throws Exception {
        byte[] artifact = Files.readAllBytes(E2eConnectorJar.buildInto(directory, connectorId));

        try (var storage = PreparedNativeStore.create("e2e", "e2e-password"); ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);

            ControlPlane.Refusal refusal = control.registerConnectorExpectingRefusal(artifact);

            // The code, not the message: a refusal that reads right and codes wrong is the failure this
            // discriminates against, and the id is carried as a parameter so the code alone is not
            // asked to say which connector was refused.
            assertThat(refusal.code()).isEqualTo("connector.not-official");
            assertThat(refusal.status()).isEqualTo(400);
            assertThat(refusal.params()).containsEntry("connector", connectorId);
        }
    }
}
