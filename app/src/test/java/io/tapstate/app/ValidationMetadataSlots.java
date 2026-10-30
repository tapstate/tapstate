package io.tapstate.app;

import java.time.Duration;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import java.util.ArrayList;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

/** Test-owned complete pool occupancy, separate from the shared pool's transient snapshots. */
final class ValidationMetadataSlots {
    private ValidationMetadataSlots() {}
    static List<ValidationMemberResources.Metadata> acquire(ValidationMemberResources.Member member, int count, Duration bound) {
        var owned = new ArrayList<ValidationMemberResources.Metadata>();
        boolean complete = false;
        try {
            org.awaitility.Awaitility.await().atMost(bound).pollInterval(Duration.ofMillis(10)).untilAsserted(() -> {
                while (owned.size() < count) {
                    try { owned.add(member.admitMetadata()); }
                    catch (TapstateException unavailable) {
                        if (unavailable.code() != ValidationError.VALUE_LIMIT_EXCEEDED
                                || !"validation.metadataFrames".equals(unavailable.args().get("limit"))) throw unavailable;
                        break;
                    }
                }
                assertThat(owned).as("owned slots; current shared snapshot: %s", member.snapshot()).hasSize(count);
            });
            complete = true;
            return owned;
        } finally {
            if (!complete) owned.forEach(ValidationMemberResources.Metadata::close);
        }
    }
}
