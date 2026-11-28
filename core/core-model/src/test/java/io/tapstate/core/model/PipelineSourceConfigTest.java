package io.tapstate.core.model;

import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PipelineSourceConfigTest {
    private final SourceResource source = new SourceResource("source", null, "mongo",
            Map.of("host", "shared", "preImage", true), SourceMode.CDC,
            List.of(TableRef.literal("orders")), new Srs("shared", null, null, null, null), null);

    @Test void resolvingAuthoredValuesDoesNotMutateTheSource() {
        var local = pipeline(Map.of("preImage", false));
        assertThat(PipelineSourceConfig.settings(local, source)).containsEntry("host", "shared").containsEntry("preImage", false);
        assertThat(source.config()).containsEntry("preImage", true);
        assertThat(PipelineSourceConfig.settings(pipeline(Map.of()), source)).isSameAs(source.config());
        assertThatThrownBy(() -> PipelineSourceConfig.settings(local, source).put("host", "changed"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void evenAnExplicitValueEqualToTheSourceSettingHasItsOwnCaptureIdentity() {
        var unconfigured = pipeline(Map.of());
        var explicit = pipeline(Map.of("preImage", true));
        var overridden = pipeline(Map.of("preImage", false));
        String legacy = SourceCaptureIdentity.resolve(source.connector(), source.config(), source.srs().key());
        assertThat(PipelineSourceConfig.chain(unconfigured, source)).isEqualTo(legacy);
        assertThat(PipelineSourceConfig.chain(explicit, source)).isNotEqualTo(legacy);
        assertThat(PipelineSourceConfig.overrides(explicit, source)).containsEntry("preImage", true);
        assertThat(PipelineSourceConfig.chain(overridden, source)).isNotEqualTo(legacy);
        assertThat(PipelineSourceConfig.physical(overridden, source)).isNotEqualTo(PipelineSourceConfig.physical(unconfigured, source));
        assertThat(SourceCaptureIdentity.resolve(source.connector(), source.config(), null, Map.of("preImage", true)))
                .isNotEqualTo(SourceCaptureIdentity.resolve(source.connector(), source.config(), null));
    }

    private PipelineResource pipeline(Map<String, Object> config) {
        return new PipelineResource("pipeline", null, List.of(new SourceRef.Spec("source", true, config)),
                null, null, null, null, null);
    }
}
