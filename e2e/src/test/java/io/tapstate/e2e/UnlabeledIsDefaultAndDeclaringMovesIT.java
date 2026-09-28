package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.tapstate.e2e.ProjectDocuments.batch;
import static io.tapstate.e2e.ProjectDocuments.pipelineWritingTo;
import static io.tapstate.e2e.ProjectDocuments.projectOf;
import static io.tapstate.e2e.ProjectDocuments.source;
import static io.tapstate.e2e.ProjectDocuments.target;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A resource with no project label is the Default project's, and the read face says so. It leaves the
 * Default project only when a project declares it; a project that merely refers to it leaves it where it
 * is, so what every project writes through stays shared.
 */
class UnlabeledIsDefaultAndDeclaringMovesIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void declaringMovesAResourceOutOfTheDefaultProjectAndReferringDoesNot() {
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_project_default"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.apply(batch("orders_db", source("orders_db"), "cluster_atlas", target("cluster_atlas")));

            ControlPlane.Project before = control.projects().get(0);
            assertThat(before.id()).as("the Default project is listed first").isEqualTo("default");
            assertThat(before.resourceIds()).containsExactlyInAnyOrder("orders_db", "cluster_atlas");

            // Declared by bank_c360 (orders_db is in its batch), referred to only (cluster_atlas is not).
            control.applyAsProject("bank_c360", batch(
                    "orders_db", source("orders_db"),
                    "customer_360", pipelineWritingTo("customer_360", "orders_db", "cluster_atlas")));

            assertThat(projectOf(control, "orders_db")).as("declared, so moved").isEqualTo("bank_c360");
            assertThat(projectOf(control, "cluster_atlas")).as("referred to, so left").isNull();
            assertThat(control.projects().get(0).resourceIds())
                    .as("what the Default project holds afterwards")
                    .containsExactly("cluster_atlas");
        }
    }
}
