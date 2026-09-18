package io.tapstate.app;

import com.hazelcast.core.HazelcastException;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Starting the embedded Hazelcast member can fail for operational reasons — most commonly the
 * loopback member port is already held by another server on the same host. That is a user-facing,
 * diagnosable startup failure, so it surfaces as a coded diagnostic (rendered by
 * {@code CodedFailureAnalyzer}) rather than a bare Hazelcast stack trace. A failure that is not
 * Hazelcast's — a programmer error while assembling the member — is not laundered into a code; it
 * crashes bare.
 */
class HazelcastStartupTest {

    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withUserConfiguration(HazelcastConfiguration.class)
            .withPropertyValues(
                    "tapstate.hz.discovery.mode=tcp-ip",
                    "tapstate.hz.discovery.tcp-ip.seeds[0]=127.0.0.1:5781",
                    "tapstate.hz.member-port=5781");

    @Test
    void clusterDiscoveryCannotStartWithoutStableIdentity() {
        context.run(started -> {
            assertThat(started).hasFailed();
            Throwable root = rootCause(started.getStartupFailure());
            assertThat(root).isInstanceOfSatisfying(TapstateException.class,
                    coded -> assertThat(coded.code()).isEqualTo(BootError.CLUSTER_ID_REQUIRED));
        });
    }

    @Test
    void memberStartupFailureBecomesACodedDiagnostic() {
        HazelcastException cause = new HazelcastException("Ports [5701-5801] are already in use");

        Throwable thrown = catchThrowable(
                () -> HazelcastConfiguration.startMember(() -> {
                    throw cause;
                }));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        TapstateException coded = (TapstateException) thrown;
        assertThat(coded.code()).isEqualTo(BootError.HAZELCAST_UNAVAILABLE);
        assertThat(coded.getCause()).isSameAs(cause);
    }

    @Test
    void aNonHazelcastFailureIsNotLaunderedIntoACode() {
        RuntimeException bug = new IllegalStateException("invariant violated while building the member");

        assertThatThrownBy(() -> HazelcastConfiguration.startMember(() -> {
            throw bug;
        })).isSameAs(bug);
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root;
    }
}
