package io.tapstate.app;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.ProjectLabel;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A project claims what it declares and nothing it only refers to. The connection a cluster is created
 * with is written through by every project on it; if the first project to apply a pipeline writing
 * there adopted it, every later project would be refused an id it never declared.
 */
@RequiresDocker
class ReferencedSharedSourceIsNotClaimedIT {

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void twoProjectsWriteThroughTheSameConnectionAndNeitherOwnsIt() {
        try (ProjectApplyServer server = ProjectApplyServer.start(REPLICA_SET)) {
            assertThat(server.apply(null, ProjectFixtures.target("cluster_atlas")).status()).isEqualTo(200);

            assertThat(server.apply("bank_c360",
                    ProjectFixtures.source("core_banking"),
                    ProjectFixtures.pipelineWritingTo("customer_360", "core_banking", "cluster_atlas"))
                    .status()).isEqualTo(200);
            assertThat(server.apply("orders_sync",
                    ProjectFixtures.source("orders_db"),
                    ProjectFixtures.pipelineWritingTo("orders_out", "orders_db", "cluster_atlas"))
                    .status()).isEqualTo(200);

            assertThat(ProjectLabel.of(new DslParser().parse(server.canonical("cluster_atlas")))).isNull();
            assertThat(ProjectLabel.of(new DslParser().parse(server.canonical("customer_360"))))
                    .isEqualTo("bank_c360");
            assertThat(ProjectLabel.of(new DslParser().parse(server.canonical("orders_out"))))
                    .isEqualTo("orders_sync");
        }
    }
}
