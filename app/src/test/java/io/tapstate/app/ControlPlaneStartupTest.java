package io.tapstate.app;

import io.tapstate.adapters.mongostore.StoreError;
import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.BootstrapService;
import io.tapstate.control.core.ConnectionTestService;
import io.tapstate.control.core.LoginService;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.PipelineLayoutService;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The control plane is wired into startup on top of the store, and it fails closed. Gated on the same
 * store switch, it comes up only when the store does: disabled, the context starts with none of the
 * control-plane services — and therefore none of the HTTP verb surface, since the controllers and the
 * authentication interceptor that guards them are imported together as one bundle, so the surface can
 * never be served unguarded. That store-disabled case is the load-bearing fail-safe proof. Enabled but
 * pointed at an unreachable store, the assembled context still surfaces the {@code store.unreachable}
 * coded diagnostic at startup rather than masking it behind a bare driver failure.
 */
class ControlPlaneStartupTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(StoreConfiguration.class, ControlPlaneConfiguration.class);

    @Test
    void disabledStartsWithoutTheControlPlaneOrItsVerbSurface() {
        runner.withPropertyValues("tapstate.store.mongo.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    // No store means no control plane at all: none of its services, so none of the /api
                    // controllers or the interceptor either (they are imported as one bundle with these).
                    assertThat(context).doesNotHaveBean(OperationRegistry.class);
                    assertThat(context).doesNotHaveBean(LoginService.class);
                    assertThat(context).doesNotHaveBean(BootstrapService.class);
                    assertThat(context).doesNotHaveBean(ApplyService.class);
                    assertThat(context).doesNotHaveBean(PipelineLayoutService.class);
                    // The connection-test verb's service is part of the same Mongo-gated plane, not unconditional.
                    assertThat(context).doesNotHaveBean(ConnectionTestService.class);
                });
    }

    @Test
    void enabledButUnreachableFailsStartupWithACodedDiagnostic() {
        // A plaintext URI at a dead port: the control plane stands on the store connection, so the
        // store-unreachable coded diagnostic surfaces at startup rather than a bare driver stack trace.
        runner.withPropertyValues(
                        "tapstate.store.mongo.enabled=true",
                        "tapstate.store.mongo.uri=mongodb://localhost:1/tapstate",
                        "tapstate.store.mongo.server-selection-timeout=300ms")
                .run(context -> {
                    assertThat(context).hasFailed();
                    TapstateException coded = firstCauseOfType(context.getStartupFailure(), TapstateException.class);
                    assertThat(coded).as("startup failure carries a coded diagnostic").isNotNull();
                    assertThat(coded.code()).isEqualTo(StoreError.UNREACHABLE);
                });
    }

    private static <T extends Throwable> T firstCauseOfType(Throwable failure, Class<T> type) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (type.isInstance(t)) {
                return type.cast(t);
            }
        }
        return null;
    }
}
