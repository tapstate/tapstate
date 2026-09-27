package io.tapstate.app;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.ProjectLabel;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ids are unique per server, not per project. A second project applying an id the first one owns is
 * refused as a whole, told which project holds it, and leaves the first project's resource exactly as
 * it was - a silent overwrite would move a running pipeline from one team to another.
 */
@RequiresDocker
class SameIdInAnotherProjectIsRefusedIT {

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void theSecondProjectIsRefusedAndTheFirstIsUntouched() {
        try (ProjectApplyServer server = ProjectApplyServer.start(REPLICA_SET)) {
            assertThat(server.apply("crm_pilot",
                    ProjectFixtures.source("crm_db"),
                    ProjectFixtures.pipeline("customer_360", "crm_db")).status()).isEqualTo(200);
            String before = server.canonical("customer_360");

            ProjectApplyServer.Answer refused = server.apply("bank_c360",
                    ProjectFixtures.source("core_banking"),
                    ProjectFixtures.pipeline("customer_360", "core_banking"));

            assertThat(refused.status()).isBetween(400, 499);
            assertThat(refused.body().get("code")).isEqualTo("artifact.project-id-taken");
            @SuppressWarnings("unchecked")
            Map<String, Object> params = (Map<String, Object>) refused.body().get("params");
            assertThat(params)
                    .containsEntry("id", "customer_360")
                    .containsEntry("owner", "crm_pilot")
                    .containsEntry("project", "bank_c360");
            assertThat(server.canonical("customer_360")).isEqualTo(before);
            assertThat(ProjectLabel.of(new DslParser().parse(before))).isEqualTo("crm_pilot");
        }
    }

    @Test
    void theSameProjectAppliesItsOwnIdAgain() {
        try (ProjectApplyServer server = ProjectApplyServer.start(REPLICA_SET)) {
            String[] batch = {ProjectFixtures.source("crm_db"), ProjectFixtures.pipeline("customer_360", "crm_db")};
            assertThat(server.apply("crm_pilot", batch).status()).isEqualTo(200);

            assertThat(server.apply("crm_pilot", batch).status()).isEqualTo(200);
        }
    }
}
