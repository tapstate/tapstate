package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@RequiresDocker
class RealConnectorNativeStorageSetupIT {
    @Test
    void theRealConnectorSetupReachesBusinessReadinessBeforeLoadingAConnector() {
        // Isolate the startup sequence used by the real-connector witnesses. No connector
        // is loaded: authentication must succeed before connector compatibility can matter.
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);

            assertThat(control.storageStatus().get("businessPhase")).isEqualTo("READY");
        }
    }
}
