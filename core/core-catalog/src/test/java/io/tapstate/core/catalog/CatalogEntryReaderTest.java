package io.tapstate.core.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.WriteMode;

/**
 * Pins the bundled catalog entry format by reading a representative document. The build tool's
 * writer (a later slice) must produce exactly this shape.
 */
class CatalogEntryReaderTest {

    private static final String MYSQL_ENTRY = """
            {
              "id": "mysql",
              "name": "Mysql",
              "displayName": "MySQL",
              "icon": "icons/mysql.png",
              "group": "database",
              "modes": ["cdc", "snapshot"],
              "discovery": "catalog",
              "sink": {"capable": true, "writeSemantics": ["upsert", "append"]},
              "pushOut": false,
              "config": [
                {
                  "name": "deploymentMode",
                  "type": "string",
                  "label": {"en_US": "Deployment mode"},
                  "required": false,
                  "default": "standalone",
                  "secret": false,
                  "options": [
                    {"value": "standalone", "label": {"en_US": "Single machine deployment"}},
                    {"value": "master-slave", "label": {"en_US": "Master-Slave Architecture"}}
                  ],
                  "visibleWhen": null
                },
                {
                  "name": "host",
                  "type": "string",
                  "label": {"en_US": "Host"},
                  "required": true,
                  "default": null,
                  "secret": false,
                  "options": [],
                  "visibleWhen": {"controllingField": "deploymentMode", "equalsAnyOf": ["standalone"]}
                },
                {
                  "name": "password",
                  "type": "string",
                  "label": {"en_US": "password"},
                  "required": false,
                  "default": null,
                  "secret": true,
                  "options": [],
                  "visibleWhen": null
                }
              ],
              "provenance": {
                "specPath": "mysql-connector/src/main/resources/mysql-spec.json",
                "specContentHash": "abc123",
                "pdkApiVersion": null,
                "requiredLevel": null,
                "modeSource": {"cdc": "derived", "snapshot": "derived"}
              }
            }
            """;

    @Test
    void readsIdentityGroupAndAxes() {
        ConnectorCatalogEntry entry = CatalogEntryReader.read(MYSQL_ENTRY);

        assertThat(entry.id()).isEqualTo("mysql");
        assertThat(entry.name()).isEqualTo("Mysql");
        assertThat(entry.displayName()).isEqualTo("MySQL");
        assertThat(entry.icon()).isEqualTo("icons/mysql.png");
        assertThat(entry.group()).isEqualTo(ConnectorGroup.DATABASE);
        assertThat(entry.modes()).containsExactly(SourceMode.CDC, SourceMode.SNAPSHOT);
        assertThat(entry.discovery()).isEqualTo(Discovery.CATALOG);
        assertThat(entry.pushOut()).isFalse();
    }

    @Test
    void readsSink() {
        ConnectorCatalogEntry entry = CatalogEntryReader.read(MYSQL_ENTRY);

        assertThat(entry.sink().capable()).isTrue();
        assertThat(entry.sink().writeSemantics()).containsExactly(WriteMode.UPSERT, WriteMode.APPEND);
    }

    @Test
    void readingALegacySourceOnlyRowWithdrawsItsSinkWithoutChangingTheDocument() {
        Map<String, Object> legacy = CatalogEntryWriter.toTree(CatalogEntryReader.read(MYSQL_ENTRY));
        legacy.put("id", "db2");

        ConnectorCatalogEntry entry = CatalogEntryReader.fromTree(legacy);

        assertThat(entry.sink().capable()).isFalse();
        assertThat(entry.sink().writeSemantics()).isEmpty();
        Map<String, Object> expected = new LinkedHashMap<>(legacy);
        expected.put("sink", Map.of("capable", false, "writeSemantics", List.of()));
        assertThat(CatalogEntryWriter.toTree(entry)).isEqualTo(expected);
        assertThat(legacy.get("sink"))
                .isEqualTo(Map.of("capable", true, "writeSemantics", List.of("upsert", "append")));
    }

    @Test
    void aSourceOnlyPolicyDoesNotHideAnUnreadableStoredSink() {
        String unreadable = MYSQL_ENTRY.replace("\"id\": \"mysql\"", "\"id\": \"db2\"")
                .replace("\"capable\": true", "\"capable\": \"true\"");

        assertThatThrownBy(() -> CatalogEntryReader.read(unreadable))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a boolean");
    }

    @Test
    void readsConfigFieldsInOrderWithLabelsRequiredSecretAndDefault() {
        ConnectorCatalogEntry entry = CatalogEntryReader.read(MYSQL_ENTRY);

        assertThat(entry.config()).extracting(ConfigField::name)
                .containsExactly("deploymentMode", "host", "password");
        ConfigField host = entry.config().get(1);
        assertThat(host.type()).isEqualTo(ConfigType.STRING);
        assertThat(host.required()).isTrue();
        assertThat(host.secret()).isFalse();
        assertThat(host.defaultValue()).isNull();
        assertThat(host.label()).containsEntry("en_US", "Host").doesNotContainKey("zh_CN");
        assertThat(entry.config().get(2).secret()).isTrue();
        assertThat(entry.config().get(0).defaultValue()).isEqualTo("standalone");
    }

    @Test
    void readsEnumOptionsWithLabels() {
        ConfigField deploymentMode = CatalogEntryReader.read(MYSQL_ENTRY).config().get(0);

        assertThat(deploymentMode.options()).extracting(EnumOption::value)
                .containsExactly("standalone", "master-slave");
        assertThat(deploymentMode.options().get(0).label())
                .containsEntry("en_US", "Single machine deployment");
    }

    @Test
    void readsVisibleWhenAndTreatsNullAsAbsent() {
        ConnectorCatalogEntry entry = CatalogEntryReader.read(MYSQL_ENTRY);

        assertThat(entry.config().get(1).visibleWhen())
                .isEqualTo(new VisibleWhen("deploymentMode", java.util.List.of("standalone")));
        assertThat(entry.config().get(0).visibleWhen()).isNull();
    }

    @Test
    void rejectsAnEntryMissingItsId() {
        String noId = MYSQL_ENTRY.replace("\"id\": \"mysql\",", "");

        assertThatThrownBy(() -> CatalogEntryReader.read(noId))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsANonBooleanWhereABooleanIsRequired() {
        // A string "false" must not be silently coerced — the catalog product is corrupt, fail loud.
        String badPush = MYSQL_ENTRY.replace("\"pushOut\": false", "\"pushOut\": \"false\"");

        assertThatThrownBy(() -> CatalogEntryReader.read(badPush))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void readsProvenanceIncludingModeSourceAndNullSlots() {
        Provenance provenance = CatalogEntryReader.read(MYSQL_ENTRY).provenance();

        // An entry document carries neither revision - they are in the index head, and a row read on
        // its own (a server-registered one) genuinely has none.
        assertThat(provenance.specSha()).isNull();
        assertThat(provenance.capabilitySha()).isNull();
        assertThat(provenance.specContentHash()).isEqualTo("abc123");
        assertThat(provenance.pdkApiVersion()).isNull();
        assertThat(provenance.modeSource())
                .containsEntry(SourceMode.CDC, ModeSource.DERIVED)
                .containsEntry(SourceMode.SNAPSHOT, ModeSource.DERIVED);
    }

    @Test
    void toTreeThenFromTreeRoundTripsAnEntryUnchanged() {
        // The writer/reader pair is the catalog product's serde; a full-fidelity round-trip (config,
        // options, visibleWhen, provenance and sink included) is what lets a registered row persist and
        // reload without drift.
        ConnectorCatalogEntry original = CatalogEntryReader.read(MYSQL_ENTRY);

        ConnectorCatalogEntry roundTripped = CatalogEntryReader.fromTree(CatalogEntryWriter.toTree(original));

        assertThat(roundTripped).isEqualTo(original);
    }
}
