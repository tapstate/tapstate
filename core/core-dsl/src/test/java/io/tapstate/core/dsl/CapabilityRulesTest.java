package io.tapstate.core.dsl;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.Resource;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Capability-matrix validation (plan poc1 C3): mode × connector legality and connector config
 * field type / enum checks, judged against the connector-derived catalog schema. The catalog is the
 * build-time projection of each connector's own spec — nothing here hard-codes connector knowledge.
 */
class CapabilityRulesTest {

    /**
     * A connector the catalog resolves no modes for, so the mode check has nothing to judge against.
     * Which connector that is belongs to the checked-in catalog, not to the rules.
     */
    private static final String NO_MODE_CONNECTOR = "elasticsearch";

    private final DslParser parser = new DslParser();
    private final TapstateCatalog catalog = TapstateCatalog.load();

    private Resource parse(String yaml) {
        return parser.parse(yaml);
    }

    @Test
    void rejectsModeOutsideConnectorCapabilityMatrix() {
        // kafka declares only [stream]; cdc is outside its matrix (§4).
        Resource src = parse("""
                version: tapstate/v1
                kind: source
                id: src_k
                connector: kafka
                config: { nameSrvAddr: "k1:9092" }
                mode: cdc
                tables: [ events ]
                """);
        assertThatThrownBy(() -> CapabilityRules.validate(List.of(src), catalog))
                .isInstanceOf(DslException.class)
                .extracting(e -> ((DslException) e).code())
                .isEqualTo(DslError.UNSUPPORTED_MODE);
    }

    @Test
    void acceptsModeWithinConnectorCapabilityMatrix() {
        // mysql declares [cdc, snapshot]; snapshot is legal.
        Resource src = parse("""
                version: tapstate/v1
                kind: source
                id: src_my
                connector: mysql
                config: { host: 10.0.0.1, username: u, password: p }
                mode: snapshot
                tables: [ orders ]
                """);
        assertThatCode(() -> CapabilityRules.validate(List.of(src), catalog))
                .doesNotThrowAnyException();
    }

    @Test
    void skipsConnectorAbsentFromCatalog() {
        // oracle is an enterprise connector, not in the bundled OSS catalog. Offline cannot judge
        // its modes — connector registration is authoritative only on the server.
        Resource src = parse("""
                version: tapstate/v1
                kind: source
                id: src_ora
                connector: oracle
                config: { host: 10.0.0.2, username: u, password: p }
                mode: cdc
                tables: [ orders ]
                """);
        assertThatCode(() -> CapabilityRules.validate(List.of(src), catalog))
                .doesNotThrowAnyException();
    }

    @Test
    void skipsModeCheckWhenNonDatabaseModesAreOnlyDerived() {
        // quickapi is a SaaS connector whose real mode (api) was never declared, so the catalog
        // only carries the derived [snapshot] — an artifact, not its capability. Offline cannot
        // trust it, so mode is deferred to the server rather than wrongly rejected.
        Resource src = parse("""
                version: tapstate/v1
                kind: source
                id: src_q
                connector: quickapi
                config: { jsonTxt: "{}" }
                mode: api
                tables: [ items ]
                """);
        assertThatCode(() -> CapabilityRules.validate(List.of(src), catalog))
                .doesNotThrowAnyException();
    }

    @Test
    void theNoModeFixtureStillHasNoModes() {
        // The test below needs a connector the catalog resolves no modes at all for, and that is a
        // property of the checked-in catalog rather than of the rules: a refresh can take it away.
        // postgres was this fixture until its capabilities became derivable, and the way that
        // surfaced was invisible - the test kept passing, because cdc had become a LEGAL mode rather
        // than an unjudged one, so the branch it names stopped being exercised and nothing said so.
        // Assert the premise directly, the same way the wizard's fixture does.
        assertThat(catalog.byId(NO_MODE_CONNECTOR).modes())
                .as("pick another connector with no modes for the skip-the-mode-check fixture")
                .isEmpty();
    }

    @Test
    void skipsModeCheckWhenConnectorHasNoModes() {
        // A connector the catalog carries no modes for: there is no offline signal to judge the
        // declared mode against, so the check is skipped rather than failed.
        Resource src = parse("""
                version: tapstate/v1
                kind: source
                id: src_es
                connector: %s
                config: { host: 10.0.0.5, port: "9200", user: u, password: p }
                mode: cdc
                tables: [ orders ]
                """.formatted(NO_MODE_CONNECTOR));
        assertThatCode(() -> CapabilityRules.validate(List.of(src), catalog))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsConfigValueWithWrongType() {
        // mysql.masterSlaveAddress is an array field; a scalar string is the wrong type.
        Resource src = parse("""
                version: tapstate/v1
                kind: source
                id: src_my
                connector: mysql
                config: { deploymentMode: master-slave, masterSlaveAddress: "10.0.0.1:3306", username: u, password: p }
                mode: snapshot
                tables: [ orders ]
                """);
        assertThatThrownBy(() -> CapabilityRules.validate(List.of(src), catalog))
                .isInstanceOf(DslException.class)
                .extracting(e -> ((DslException) e).code())
                .isEqualTo(DslError.CONFIG_TYPE_MISMATCH);
    }

    @Test
    void rejectsConfigValueOutsideEnum() {
        // mysql.deploymentMode is an enum [standalone, master-slave]; "cluster" is not a choice.
        Resource src = parse("""
                version: tapstate/v1
                kind: source
                id: src_my
                connector: mysql
                config: { deploymentMode: cluster, host: 10.0.0.1, username: u, password: p }
                mode: snapshot
                tables: [ orders ]
                """);
        assertThatThrownBy(() -> CapabilityRules.validate(List.of(src), catalog))
                .isInstanceOf(DslException.class)
                .extracting(e -> ((DslException) e).code())
                .isEqualTo(DslError.INVALID_CONFIG_VALUE);
    }

    @Test
    void acceptsValidArrayEnumSelection() {
        // dummy.incremental_types is a multi-select array enum [1,2,3]; a legal subset is valid.
        Resource src = parse("""
                version: tapstate/v1
                kind: source
                id: src_d
                connector: dummy
                config: { incremental_types: ["1", "2"] }
                mode: snapshot
                tables: [ t ]
                """);
        assertThatCode(() -> CapabilityRules.validate(List.of(src), catalog))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsArrayEnumElementOutsideChoices() {
        // a single out-of-range element of a multi-select array enum is rejected.
        Resource src = parse("""
                version: tapstate/v1
                kind: source
                id: src_d
                connector: dummy
                config: { incremental_types: ["1", "9"] }
                mode: snapshot
                tables: [ t ]
                """);
        assertThatThrownBy(() -> CapabilityRules.validate(List.of(src), catalog))
                .isInstanceOf(DslException.class)
                .extracting(e -> ((DslException) e).code())
                .isEqualTo(DslError.INVALID_CONFIG_VALUE);
    }

    @Test
    void rejectsIllegalModeForDatabaseConnector() {
        // mysql is a database with derived-only modes [cdc, snapshot]; stream is illegal. This
        // routes the rejection through the DATABASE trust branch (mysql carries no declared mode).
        Resource src = parse("""
                version: tapstate/v1
                kind: source
                id: src_my
                connector: mysql
                config: { host: 10.0.0.1, username: u, password: p }
                mode: stream
                tables: [ orders ]
                """);
        assertThatThrownBy(() -> CapabilityRules.validate(List.of(src), catalog))
                .isInstanceOf(DslException.class)
                .extracting(e -> ((DslException) e).code())
                .isEqualTo(DslError.UNSUPPORTED_MODE);
    }

    @Test
    void skipsInterpolatedConfigValue() {
        // ${ENV} externalization is opaque offline; an enum field set to ${...} is not enum-checked.
        Resource src = parse("""
                version: tapstate/v1
                kind: source
                id: src_my
                connector: mysql
                config: { deploymentMode: "${DEPLOY_MODE}", host: 10.0.0.1, username: u, password: p }
                mode: snapshot
                tables: [ orders ]
                """);
        assertThatCode(() -> CapabilityRules.validate(List.of(src), catalog))
                .doesNotThrowAnyException();
    }

    @Test
    void toleratesUnknownConfigKey() {
        // A key not in the connector's normalized schema is passed through (the spec may drop
        // fields); offline does not reject it.
        Resource src = parse("""
                version: tapstate/v1
                kind: source
                id: src_my
                connector: mysql
                config: { host: 10.0.0.1, username: u, password: p, notARealMysqlField: x }
                mode: snapshot
                tables: [ orders ]
                """);
        assertThatCode(() -> CapabilityRules.validate(List.of(src), catalog))
                .doesNotThrowAnyException();
    }

    @Test
    void skipsWriteTargetWithoutMode() {
        // A pure connection supplier (X18) omits mode; there is no read mode to judge.
        Resource tgt = parse("""
                version: tapstate/v1
                kind: source
                id: tgt_es
                connector: elasticsearch
                config: { host: "http://10.0.0.9:9200", username: w, password: p }
                """);
        assertThatCode(() -> CapabilityRules.validate(List.of(tgt), catalog))
                .doesNotThrowAnyException();
    }

    @Test
    void onlineValidationRejectsMissingStaticRequiredConfig() {
        Resource src = parse("""
                version: tapstate/v1
                kind: source
                id: src_my
                connector: mysql
                config: { host: 10.0.0.1, port: 3306, username: u }
                mode: snapshot
                tables: [ orders ]
                """);

        assertThatThrownBy(() -> CapabilityRules.validateOnline((io.tapstate.core.model.SourceResource) src, catalog))
                .isInstanceOf(DslException.class)
                .extracting(e -> ((DslException) e).code())
                .isEqualTo(DslError.CONFIG_REQUIRED);
    }

    @Test
    void onlineValidationUsesConnectorVisibilityForMongoConnectionModes() {
        Resource uri = parse("""
                version: tapstate/v1
                kind: source
                id: src_uri
                connector: mongodb
                config: { isUri: true, host: ignored }
                mode: snapshot
                tables: [ orders ]
                """);
        assertThatThrownBy(() -> CapabilityRules.validateOnline((io.tapstate.core.model.SourceResource) uri, catalog))
                .isInstanceOf(DslException.class)
                .extracting(e -> ((DslException) e).code())
                .isEqualTo(DslError.CONFIG_REQUIRED);

        Resource standard = parse("""
                version: tapstate/v1
                kind: source
                id: src_standard
                connector: mongodb
                config: { isUri: false, host: localhost, database: orders }
                mode: snapshot
                tables: [ orders ]
                """);
        assertThatCode(() -> CapabilityRules.validateOnline(
                (io.tapstate.core.model.SourceResource) standard, catalog))
                .doesNotThrowAnyException();
    }

    @Test
    void onlineValidationRejectsNullRequiredValues() {
        Resource src = parse("""
                version: tapstate/v1
                kind: source
                id: src_null
                connector: mongodb
                config: { isUri: false, host: null, database: orders }
                mode: snapshot
                tables: [ orders ]
                """);

        assertThatThrownBy(() -> CapabilityRules.validateOnline(
                (io.tapstate.core.model.SourceResource) src, catalog))
                .isInstanceOf(DslException.class)
                .extracting(e -> ((DslException) e).code())
                .isEqualTo(DslError.CONFIG_TYPE_MISMATCH);
    }
}
