package io.tapstate.app;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.ProjectLabel;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A project brought up on a server leaves every resource it declares labelled with its id - the
 * sources as well as the pipelines, since the web groups both and a source left out would sit under
 * no project while its pipeline sat under one.
 */
@RequiresDocker
class ApplyStampsProjectLabelIT {

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void everyResourceAProjectDeclaresCarriesItsId() {
        try (ProjectApplyServer server = ProjectApplyServer.start(REPLICA_SET)) {
            ProjectApplyServer.Answer applied = server.apply("bank_c360",
                    ProjectFixtures.source("core_banking"),
                    ProjectFixtures.pipeline("customer_360", "core_banking"),
                    ProjectFixtures.pipeline("account_360", "core_banking"));

            assertThat(applied.status()).isEqualTo(200);
            for (String id : List.of("core_banking", "customer_360", "account_360")) {
                assertThat(ProjectLabel.of(new DslParser().parse(server.canonical(id))))
                        .as("%s is declared by the project, so it belongs to it", id)
                        .isEqualTo("bank_c360");
            }
        }
    }

    @Test
    void aBatchThatNamesNoProjectIsStoredAsWritten() {
        try (ProjectApplyServer server = ProjectApplyServer.start(REPLICA_SET)) {
            assertThat(server.apply(null, ProjectFixtures.source("core_banking")).status()).isEqualTo(200);

            assertThat(ProjectLabel.of(new DslParser().parse(server.canonical("core_banking")))).isNull();
        }
    }
}
