package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.tapstate.e2e.ProjectDocuments.batch;
import static io.tapstate.e2e.ProjectDocuments.pipeline;
import static io.tapstate.e2e.ProjectDocuments.source;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every server keeps its Default project: it is listed from the first start even when empty, it can be
 * emptied resource by resource, and it cannot be removed as a whole. Any other project can be.
 */
class DefaultProjectCannotBeRemovedIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void theDefaultProjectIsRefusedAndItsResourcesStillDeleteOneByOne() {
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_project_keep_default"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");

            assertThat(control.projects()).as("a fresh server already lists it, empty")
                    .extracting(ControlPlane.Project::id).containsExactly("default");

            control.apply(batch("orders_db", source("orders_db")));
            ControlPlane.Refusal refused = control.removeProjectExpectingRefusal("default");

            assertThat(refused.status()).isEqualTo(409);
            assertThat(refused.code()).isEqualTo("artifact.default-project-not-removable");
            assertThat(control.artifactIds()).contains("orders_db");

            control.deleteArtifact("orders_db", control.artifact("orders_db").orElseThrow().contentHash());
            assertThat(control.artifactIds()).doesNotContain("orders_db");
            assertThat(control.projects()).extracting(ControlPlane.Project::id).containsExactly("default");
        }
    }

    @Test
    void anotherProjectIsRemovedWithEverythingItHolds() {
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_project_remove"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.applyAsProject("c360_sample", batch(
                    "bank_core", source("bank_core"), "customer_360", pipeline("customer_360", "bank_core")));

            control.removeProject("c360_sample");

            assertThat(control.artifactIds()).doesNotContain("bank_core", "customer_360");
            assertThat(control.projects()).extracting(ControlPlane.Project::id).containsExactly("default");
        }
    }
}
