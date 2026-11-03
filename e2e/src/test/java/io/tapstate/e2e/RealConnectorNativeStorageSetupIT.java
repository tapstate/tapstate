package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import java.util.LinkedHashMap;
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

            assertThat(new LinkedHashMap<Object, Object>(control.storageStatus())).containsEntry("businessPhase", "READY");
        }
    }
    @Test void anUnresolvableDefaultSrvTargetDoesNotGateThePreparedCacheLogin() {
        // A retained binding must not resolve the default target while assembling CACHE and authentication.
        String cluster = "cache-first-" + java.util.UUID.randomUUID();
        var target = new io.tapstate.core.model.SourceResource("views", null, "mongodb",
                java.util.Map.of("isUri", true, "uri", "mongodb+srv://user:unused@unresolvable.example.invalid/views?tls=true"),
                null, null, null, null);
        try (PreparedNativeStore storage = PreparedNativeStore.withBorrowedMaster(cluster, "e2e", "e2e-password", target);
                ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            var snapshot = new LinkedHashMap<Object,Object>(control.storageStatus());
            assertThat(snapshot).containsEntry("businessPhase", "READY").containsEntry("defaultSourceId", "views");
            assertThat(((java.util.Map<?,?>)snapshot.get("applicationDataVerification")).get("status")).isEqualTo("UNKNOWN");
        }
    }

}
