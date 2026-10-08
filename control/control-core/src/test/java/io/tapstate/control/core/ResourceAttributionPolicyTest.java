package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResourceAttributionPolicyTest {

    private static final String USER_ID = "8b7b68c8-75b5-4ba2-9b5b-3f8331265a4c";
    private final DslParser parser = new DslParser();
    private final CanonicalWriter writer = new CanonicalWriter();

    @Test
    void cloudCreatesMarkEveryTopLevelResourceKindWithTheVerifiedUserId() {
        ResourceAttributionPolicy policy = ResourceAttributionPolicy.managedCloud();

        for (Resource resource : resources()) {
            Resource attributed = policy.attribute(USER_ID, resource, null);
            assertThat(attributed.metadata().cloud()).isTrue();
            assertThat(attributed.metadata().userId()).isEqualTo(USER_ID);
            assertThat(writer.write(attributed))
                    .contains("cloud: true", "user_id: " + USER_ID);
        }
    }

    @Test
    void onPremCreatesDoNotGainCloudAttribution() {
        Resource source = resources().getFirst();

        Resource unchanged = ResourceAttributionPolicy.onPrem().attribute("admin", source, null);

        assertThat(unchanged).isSameAs(source);
        assertThat(unchanged.metadata().cloud()).isNull();
        assertThat(unchanged.metadata().userId()).isNull();
    }

    @Test
    void callersCannotChooseAttributionOnCreate() {
        Resource forged = parser.parse("""
                version: tapstate/v1
                kind: source
                id: forged
                metadata:
                  cloud: true
                  user_id: someone-else
                connector: mysql
                config: {}
                """);

        assertThatThrownBy(() -> ResourceAttributionPolicy.managedCloud().attribute(USER_ID, forged, null))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
    }

    @Test
    void anUpdatePreservesOriginalAttributionEvenWhenMetadataIsOmitted() {
        Resource original = ResourceAttributionPolicy.managedCloud()
                .attribute(USER_ID, resources().getFirst(), null);
        Resource submitted = parser.parse("""
                version: tapstate/v1
                kind: source
                id: source_a
                connector: mysql
                config: { host: changed.example }
                """);

        Resource updated = ResourceAttributionPolicy.managedCloud()
                .attribute("another-user", submitted, original);

        assertThat(updated.metadata().cloud()).isTrue();
        assertThat(updated.metadata().userId()).isEqualTo(USER_ID);
    }

    @Test
    void anUpdateCannotReplaceOriginalAttribution() {
        Resource original = ResourceAttributionPolicy.managedCloud()
                .attribute(USER_ID, resources().getFirst(), null);
        Resource forged = parser.parse("""
                version: tapstate/v1
                kind: source
                id: source_a
                metadata:
                  cloud: true
                  user_id: another-user
                connector: mysql
                config: {}
                """);

        assertThatThrownBy(() -> ResourceAttributionPolicy.managedCloud()
                        .attribute(USER_ID, forged, original))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
    }

    private List<Resource> resources() {
        return List.of(
                parser.parse("""
                        version: tapstate/v1
                        kind: source
                        id: source_a
                        metadata: { description: Source }
                        connector: mysql
                        config: {}
                        """),
                parser.parse("""
                        version: tapstate/v1
                        kind: pipeline
                        id: pipeline_a
                        metadata: { description: Pipeline }
                        source: []
                        """),
                parser.parse("""
                        version: tapstate/v1
                        kind: transform
                        id: transform_a
                        metadata: { description: Transform }
                        type: filter
                        expr: "true"
                        """),
                parser.parse("""
                        version: tapstate/v1
                        kind: view
                        id: view_a
                        metadata: { description: View }
                        primary_key: id
                        """),
                parser.parse("""
                        version: tapstate/v1
                        kind: serve
                        id: serve_a
                        metadata: { description: Serve }
                        """));
    }
}
