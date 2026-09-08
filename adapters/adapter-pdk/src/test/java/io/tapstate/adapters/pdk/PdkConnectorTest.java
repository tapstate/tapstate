package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The opened-connector handle. A synthetic connector binds to the frozen contract alone, so it opens
 * without the PDK runtime and lets the driving context be checked in the default build.
 */
class PdkConnectorTest {

    private static final String APP_TYPE = "app_type";

    private static ConnectorRef ref(Path dir) {
        return new ConnectorRef(
                List.of(Synthetic.discoverableSource(dir)), "synthetic.Discoverable", "2.0.8", null);
    }

    @Test
    void openingAConnectorDeclaresTheHostDeploymentIdentityWhenTheHostChoseNone(@TempDir Path dir) {
        String saved = System.getProperty(APP_TYPE);
        System.clearProperty(APP_TYPE);
        try (PdkConnector connector = PdkConnector.open("demo", ref(dir), Map.of())) {
            // A real connector's runtime reads its deployment identity (app_type) to pick on-prem vs cloud
            // behaviour and crashes constructing its writer when it is blank. Opening any connector declares
            // the host identity, so a real connector never meets a blank one. Tapstate hosts as standalone.
            assertThat(System.getProperty(APP_TYPE)).isEqualTo("DAAS");
        } finally {
            restore(APP_TYPE, saved);
        }
    }

    @Test
    void openingAConnectorLeavesAnAlreadyChosenDeploymentIdentityInPlace(@TempDir Path dir) {
        String saved = System.getProperty(APP_TYPE);
        System.setProperty(APP_TYPE, "DRS");
        try (PdkConnector connector = PdkConnector.open("demo", ref(dir), Map.of())) {
            // An operator or a cloud host may have chosen a deployment identity already; the host default
            // fills a gap only, it never overrides a chosen one.
            assertThat(System.getProperty(APP_TYPE)).isEqualTo("DRS");
        } finally {
            restore(APP_TYPE, saved);
        }
    }

    private static void restore(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    @Test
    void theDrivingContextCarriesAUsableStateMap(@TempDir Path dir) {
        try (PdkConnector connector = PdkConnector.open("demo", ref(dir), Map.of())) {
            // A connector reads and writes per-run scratch through the context's state map during init and
            // discovery; a null one is an NPE the moment it touches one. The map has to be there and work,
            // not merely be present.
            KVMap<Object> stateMap = connector.context().getStateMap();
            assertThat(stateMap).as("driving context state map").isNotNull();
            stateMap.put("cursor", "42");
            assertThat(stateMap.get("cursor")).isEqualTo("42");
        }
    }

    @Test
    void fillsAFieldsPdkTypeFromTheSpecsDataTypesMapping(@TempDir Path dir) {
        // A discovered field carries only its database type ("int"); a connector reads by the PDK type the
        // connector's spec maps it onto. A ref carrying that spec fills the field's tapType, which is null
        // as discovered.
        String spec = "{\"dataTypes\": {\"int\": {\"to\": \"TapNumber\", \"bit\": 32}}}";
        ConnectorRef ref = new ConnectorRef(
                List.of(Synthetic.discoverableSource(dir)), "synthetic.Discoverable", "2.0.8", null, spec);
        try (PdkConnector connector = PdkConnector.open("demo", ref, Map.of())) {
            TapTable table = new TapTable("orders");
            table.add(new TapField("id", "int"));
            assertThat(table.getNameFieldMap().get("id").getTapType()).as("tapType before fill").isNull();

            connector.fillFieldTypes(table);

            assertThat(table.getNameFieldMap().get("id").getTapType()).as("tapType after fill").isNotNull();
        }
    }

    @Test
    void theDrivingContextsSpecificationCarriesTheDataTypesMapping(@TempDir Path dir) {
        String spec = "{\"dataTypes\": {\"int\": {\"to\": \"TapNumber\", \"bit\": 32}}}";
        ConnectorRef ref = new ConnectorRef(
                List.of(Synthetic.discoverableSource(dir)), "synthetic.Discoverable", "2.0.8", null, spec);
        try (PdkConnector connector = PdkConnector.open("demo", ref, Map.of())) {
            // A connector reads its own type mapping off the specification it is driven with, not off
            // anything the host keeps to itself. Left null there, the first read of it throws inside the
            // connector on a path the host only sees as a stack in the log - the change stream starts
            // anyway and the rows keep coming, so nothing fails.
            assertThat(connector.context().getSpecification().getDataTypesMap())
                    .as("the mapping the connector itself reads")
                    .isNotNull()
                    // Present is not the same as right: an empty one is also present, and would leave the
                    // connector resolving nothing while the null check above passed.
                    .satisfies(map -> assertThat(map.get("int"))
                            .as("the type this spec declared, resolved through it").isNotNull());
        }
    }

    @Test
    void fillFieldTypesLeavesTheFieldsUntouchedForAConnectorWithNoSpec(@TempDir Path dir) {
        // A connector with no spec declares no mapping, so there is nothing to fill and the fields stay as
        // discovered - the tapType null.
        ConnectorRef ref = new ConnectorRef(
                List.of(Synthetic.discoverableSource(dir)), "synthetic.Discoverable", "2.0.8", null);
        try (PdkConnector connector = PdkConnector.open("demo", ref, Map.of())) {
            TapTable table = new TapTable("orders");
            table.add(new TapField("id", "int"));

            connector.fillFieldTypes(table);

            assertThat(table.getNameFieldMap().get("id").getTapType()).isNull();
        }
    }

    @Test
    void theDrivingContextCarriesConnectorCapabilities(@TempDir Path dir) {
        try (PdkConnector connector = PdkConnector.open("demo", ref(dir), Map.of())) {
            // A connector reads its capability alternatives off the context during the drive; a null set is
            // an NPE the moment it reads one. An empty set means no overrides - the connector's own defaults.
            assertThat(connector.context().getConnectorCapabilities())
                    .as("driving context connector capabilities").isNotNull();
        }
    }

    @Test
    void theDrivingContextCarriesAUsableGlobalStateMap(@TempDir Path dir) {
        try (PdkConnector connector = PdkConnector.open("demo", ref(dir), Map.of())) {
            // The global state map is the same shape and reached the same way; a connector that touches it
            // during init hits the same null. Provided alongside the per-run map.
            KVMap<Object> globalStateMap = connector.context().getGlobalStateMap();
            assertThat(globalStateMap).as("driving context global state map").isNotNull();
            globalStateMap.put("epoch", "1");
            assertThat(globalStateMap.get("epoch")).isEqualTo("1");
        }
    }
}
