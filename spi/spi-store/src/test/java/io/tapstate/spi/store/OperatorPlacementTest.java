package io.tapstate.spi.store;

import org.junit.jupiter.api.Test;
import java.io.*;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class OperatorPlacementTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 1, "cache", "pipeline", 1);
    private static OperatorPlacement at(PipelineIntentScope scope, String db, OperatorPlacement.Resource kind, String step, String path, String ns) {
        return new OperatorPlacement(scope, db, kind, step, path, ns);
    }
    @Test void everyPhysicalCoordinateContributesToTheHotName() {
        var original = at(SCOPE, "cache", OperatorPlacement.Resource.NEST_STATE, "step", "path", "namespace");
        var alternatives = List.of(
            at(new PipelineIntentScope("other", 1, "cache", "pipeline", 1), "cache", original.resource(), "step", "path", "namespace"),
            at(new PipelineIntentScope("cluster", 2, "cache", "pipeline", 1), "cache", original.resource(), "step", "path", "namespace"),
            at(new PipelineIntentScope("cluster", 1, "other", "pipeline", 1), "cache", original.resource(), "step", "path", "namespace"),
            at(new PipelineIntentScope("cluster", 1, "cache", "other", 1), "cache", original.resource(), "step", "path", "namespace"),
            at(new PipelineIntentScope("cluster", 1, "cache", "pipeline", 2), "cache", original.resource(), "step", "path", "namespace"),
            at(SCOPE, "other", original.resource(), "step", "path", "namespace"),
            at(SCOPE, "cache", OperatorPlacement.Resource.JOIN_FACTS, "step", "path", "namespace"),
            at(SCOPE, "cache", original.resource(), "other", "path", "namespace"),
            at(SCOPE, "cache", original.resource(), "step", "other", "namespace"),
            at(SCOPE, "cache", original.resource(), "step", "path", "other"));
        assertThat(alternatives).extracting(OperatorPlacement::mapName).doesNotContain(original.mapName()).doesNotHaveDuplicates();
        assertThat(original.mapName()).matches("nest\\.[a-f0-9]{64}");
    }
    @Test void fieldBoundariesAndUtf16NamesCannotAlias() {
        var a = at(SCOPE, "cache", OperatorPlacement.Resource.NEST_STATE, "a.b", "c", "n");
        var b = at(SCOPE, "cache", OperatorPlacement.Resource.NEST_STATE, "a", "b.c", "n");
        assertThat(a.mapName()).isNotEqualTo(b.mapName());
        assertThat(at(SCOPE, "cache", a.resource(), "step", "path", new String(new char[]{0xd800})).mapName())
            .isNotEqualTo(at(SCOPE, "cache", a.resource(), "step", "path", "?").mapName());
    }
    @Test void serializationRetainsTheExactIdentityAndStableMapName() throws Exception {
        var original = at(SCOPE, "cache", OperatorPlacement.Resource.JOIN_INDEXES, "step", "root", "index");
        var bytes = new ByteArrayOutputStream(); try (var out = new ObjectOutputStream(bytes)) { out.writeObject(original); }
        try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            var restored = (OperatorPlacement) in.readObject(); assertThat(restored).isEqualTo(original); assertThat(restored.mapName()).isEqualTo(original.mapName());
        }
    }
    @Test void incompletePlacementIsRejectedBeforeAnyStoreCanOpen() {
        assertThatIllegalArgumentException().isThrownBy(() -> at(SCOPE, "", OperatorPlacement.Resource.NEST_STATE, "step", "path", "n"));
        assertThatIllegalArgumentException().isThrownBy(() -> at(SCOPE, "cache", OperatorPlacement.Resource.NEST_STATE, "", "path", "n"));
        assertThatIllegalArgumentException().isThrownBy(() -> at(SCOPE, "cache", OperatorPlacement.Resource.JOIN_FACTS, "step", "", "n"));
        assertThatIllegalArgumentException().isThrownBy(() -> at(SCOPE, "cache", OperatorPlacement.Resource.NEST_STATE, "step", "path", ""));
    }
    @Test void aNestRootHasAnExplicitEmptyLogicalPathRatherThanASyntheticFieldName() {
        var root = at(SCOPE,"cache",OperatorPlacement.Resource.NEST_STATE,"step","","state");
        var field = at(SCOPE,"cache",OperatorPlacement.Resource.NEST_STATE,"step","root","state");
        assertThat(root.mapName()).isNotEqualTo(field.mapName());
        assertThat(root.pathId()).isEmpty();
        assertThatIllegalArgumentException().isThrownBy(() -> at(SCOPE,"cache",OperatorPlacement.Resource.NEST_STATE,"step",null,"state"));
        assertThatIllegalArgumentException().isThrownBy(() -> at(SCOPE,"cache",OperatorPlacement.Resource.NEST_STATE,"step"," ","state"));
    }

}
