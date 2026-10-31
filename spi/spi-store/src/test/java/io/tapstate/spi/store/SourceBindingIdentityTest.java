package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class SourceBindingIdentityTest {
    private static final String UUID = "11111111-1111-4111-8111-111111111111";
    @Test void identityRejectsAmbiguousUuidFormsAndNonpositiveBindings() {
        assertThat(new SourceBindingIdentity("orders", UUID, Long.MAX_VALUE).sourceId()).isEqualTo("orders");
        for (String invalid : List.of("1-1-1-1-1", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA", "11111111-1111-1111-8111-111111111111")) {
            assertThatThrownBy(() -> new SourceBindingIdentity("orders", invalid, 1)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new SourceBindingIdentity("orders", UUID, 0)).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void defaultPortDoesNotInventAnIdentityFromAnExistingResource() {
        var source = new SourceResource("orders", null, "mongodb", Map.of(), null, null, null, null);
        ArtifactStore oldBackend = new ArtifactStore() {
            @Override public void saveAll(List<Resource> artifacts) { }
            @Override public Optional<Resource> get(String id) { return Optional.of(source); }
            @Override public List<Resource> list() { return List.of(source); }
        };
        assertThatThrownBy(() -> oldBackend.sourceBinding("orders")).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.SOURCE_IDENTITY_UNAVAILABLE));
    }
    @Test void oneSnapshotCannotMixADefinitionWithAnotherSourcesLifetime() {
        var source = new SourceResource("orders", null, "mongodb", Map.of(), null, null, null, null);
        assertThatThrownBy(() -> new SourceBindingSnapshot(source, "a".repeat(64),
                new SourceBindingIdentity("customers", UUID, 1))).isInstanceOf(IllegalArgumentException.class);
    }
}
