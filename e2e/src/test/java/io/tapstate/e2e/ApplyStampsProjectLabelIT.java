package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.tapstate.e2e.ProjectDocuments.batch;
import static io.tapstate.e2e.ProjectDocuments.pipeline;
import static io.tapstate.e2e.ProjectDocuments.projectOf;
import static io.tapstate.e2e.ProjectDocuments.source;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A project brought up on a server leaves every resource it declares labelled with its id - the
 * sources as well as the pipelines, since the console groups both and a source left out would sit
 * under no project while its pipeline sat under one.
 */
class ApplyStampsProjectLabelIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void everyResourceAProjectDeclaresCarriesItsId() {
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_project_stamp"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");

            control.applyAsProject("bank_c360", batch(
                    "core_banking", source("core_banking"),
                    "customer_360", pipeline("customer_360", "core_banking"),
                    "account_360", pipeline("account_360", "core_banking")));

            for (String id : List.of("core_banking", "customer_360", "account_360")) {
                assertThat(projectOf(control, id)).as("%s is declared by the project, so it belongs to it", id)
                        .isEqualTo("bank_c360");
            }
        }
    }

    @Test
    void aBatchThatNamesNoProjectIsStoredAsWritten() {
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_project_unnamed"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");

            control.apply(batch("core_banking", source("core_banking")));

            assertThat(projectOf(control, "core_banking")).isNull();
        }
    }
}
