package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.tapstate.e2e.ProjectDocuments.batch;
import static io.tapstate.e2e.ProjectDocuments.pipeline;
import static io.tapstate.e2e.ProjectDocuments.projectOf;
import static io.tapstate.e2e.ProjectDocuments.source;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ids are unique per server, not per project. A second project applying an id the first one owns is
 * refused as a whole, told which project holds it, and leaves the first project's resource exactly as
 * it was - a silent overwrite would move a running pipeline from one team to another.
 */
class SameIdInAnotherProjectIsRefusedIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void theSecondProjectIsRefusedAndTheFirstIsUntouched() {
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_project_clash"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.applyAsProject("crm_pilot", batch(
                    "crm_db", source("crm_db"), "customer_360", pipeline("customer_360", "crm_db")));
            String before = control.artifact("customer_360").orElseThrow().canonicalForm();

            ControlPlane.Refusal refused = control.applyAsProjectExpectingRefusal("bank_c360", batch(
                    "core_banking", source("core_banking"),
                    "customer_360", pipeline("customer_360", "core_banking")));

            assertThat(refused.status()).as("the batch is valid; what refuses it is who owns the id").isEqualTo(409);
            assertThat(refused.code()).isEqualTo("artifact.project-id-taken");
            assertThat(refused.params())
                    .containsEntry("id", "customer_360")
                    .containsEntry("owner", "crm_pilot")
                    .containsEntry("project", "bank_c360");
            assertThat(control.artifact("customer_360").orElseThrow().canonicalForm()).isEqualTo(before);
            assertThat(projectOf(control, "customer_360")).isEqualTo("crm_pilot");
            assertThat(control.artifactIds()).as("nothing of the refused batch was filed").doesNotContain("core_banking");
        }
    }

    @Test
    void theSameProjectAppliesItsOwnIdAgain() {
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_project_again"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            var documents = batch("crm_db", source("crm_db"), "customer_360", pipeline("customer_360", "crm_db"));

            control.applyAsProject("crm_pilot", documents);
            control.applyAsProject("crm_pilot", documents);

            assertThat(projectOf(control, "customer_360")).isEqualTo("crm_pilot");
        }
    }
}
