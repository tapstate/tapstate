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
 * A project claims what it declares and nothing it only refers to. The connection a cluster is created
 * with is written through by every project on it; if the first project to apply a pipeline writing
 * there adopted it, every later project would be refused an id it never declared.
 */
class ReferencedSharedSourceIsNotClaimedIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void twoProjectsWriteThroughTheSameConnectionAndNeitherOwnsIt() {
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_project_shared"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.apply(batch("cluster_atlas", target("cluster_atlas")));

            control.applyAsProject("bank_c360", batch(
                    "core_banking", source("core_banking"),
                    "customer_360", pipelineWritingTo("customer_360", "core_banking", "cluster_atlas")));
            control.applyAsProject("orders_sync", batch(
                    "orders_db", source("orders_db"),
                    "orders_out", pipelineWritingTo("orders_out", "orders_db", "cluster_atlas")));

            assertThat(projectOf(control, "cluster_atlas")).as("referred to by both, declared by neither").isNull();
            assertThat(projectOf(control, "customer_360")).isEqualTo("bank_c360");
            assertThat(projectOf(control, "orders_out")).isEqualTo("orders_sync");
        }
    }
}
