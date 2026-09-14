package io.tapstate.core.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Drives the Formily normalizer against real connector spec.json fixtures (checked in under
 * {@code src/test/resources/specs/}, copied from tapdata-connectors at SHA 20371556…). A test-scope
 * JSON parser turns each fixture into the neutral tree the normalizer consumes; production code
 * parses nothing (the assembler owns that, and the core ring ships zero third-party).
 */
class SpecNormalizerTest {

    // ---- mysql: database, void container, field-driven + boolean reactions, dml policies ----

    @Test
    void readsIdentityAndTagGroup() {
        NormalizedSpec mysql = normalize("mysql");

        assertThat(mysql.id()).isEqualTo("mysql");
        assertThat(mysql.name()).isEqualTo("Mysql");
        assertThat(mysql.displayName()).isEqualTo("MySQL");
        assertThat(mysql.icon()).isEqualTo("icons/mysql.png");
        assertThat(mysql.tagGroup()).isEqualTo(ConnectorGroup.DATABASE);
    }

    @Test
    void flattensVoidContainerLeavesAndDropsTheContainerItself() {
        NormalizedSpec mysql = normalize("mysql");

        // addtionalString and timezone live inside the OPTIONAL_FIELDS void container.
        assertThat(fieldNames(mysql)).contains(
                "deploymentMode", "host", "port", "masterSlaveAddress", "database",
                "username", "password", "addtionalString", "timezone");
        // The void container is not itself a field.
        assertThat(fieldNames(mysql)).doesNotContain("OPTIONAL_FIELDS");
    }

    @Test
    void readsAStringFieldShapeWithLocalizedLabel() {
        ConfigField host = field(normalize("mysql"), "host");

        assertThat(host.type()).isEqualTo(ConfigType.STRING);
        assertThat(host.required()).isTrue();
        assertThat(host.secret()).isFalse();
        assertThat(host.label()).containsEntry("en_US", "Host").doesNotContainKey("zh_CN");
    }

    @Test
    void takesTheNumericTypeFromAnInputNumberComponent() {
        // MySQL declares port with the Formily schema type "string", but InputNumber is the
        // connector's declaration that its config bean receives a Number.
        assertThat(field(normalize("mysql"), "port").type()).isEqualTo(ConfigType.NUMBER);
    }

    @Test
    void marksPasswordComponentsSecret() {
        assertThat(field(normalize("mysql"), "password").secret()).isTrue();
        assertThat(field(normalize("mysql"), "host").secret()).isFalse();
    }

    @Test
    void resolvesEnumOptionLabelsThroughMessageRefs() {
        ConfigField deploymentMode = field(normalize("mysql"), "deploymentMode");

        assertThat(deploymentMode.options())
                .extracting(EnumOption::value)
                .containsExactly("standalone", "master-slave");
        EnumOption standalone = deploymentMode.options().get(0);
        assertThat(standalone.label())
                .containsEntry("en_US", "Single machine deployment")
                .doesNotContainKey("zh_CN");
    }

    @Test
    void invertsFieldDrivenVisibilityReactionsOntoTheirTargets() {
        NormalizedSpec mysql = normalize("mysql");

        // deploymentMode controls host/port (standalone) and masterSlaveAddress (master-slave).
        assertThat(field(mysql, "host").visibleWhen())
                .isEqualTo(new VisibleWhen("deploymentMode", List.of("standalone")));
        assertThat(field(mysql, "masterSlaveAddress").visibleWhen())
                .isEqualTo(new VisibleWhen("deploymentMode", List.of("master-slave")));
        // The controller itself is always visible.
        assertThat(field(mysql, "deploymentMode").visibleWhen()).isNull();
    }

    @Test
    void modelsAnArrayFieldAsALeafWithoutRecursingItsItems() {
        ConfigField masterSlave = field(normalize("mysql"), "masterSlaveAddress");

        assertThat(masterSlave.type()).isEqualTo(ConfigType.ARRAY);
        // The inner Space/host/port/remove of the ArrayItems are not emitted as top-level fields.
        assertThat(fieldNames(normalize("mysql"))).doesNotContain("space", "remove", "add");
    }

    @Test
    void carriesDefaultsAsCanonicalText() {
        NormalizedSpec mysql = normalize("mysql");

        assertThat(field(mysql, "port").defaultValue()).isEqualTo("3306");
        assertThat(field(mysql, "deploymentMode").defaultValue()).isEqualTo("standalone");
        assertThat(field(mysql, "addtionalString").defaultValue())
                .isEqualTo("useUnicode=yes&characterEncoding=UTF-8");
        assertThat(field(mysql, "host").defaultValue()).isNull();
    }

    @Test
    void readsDmlSinkSignals() {
        NormalizedSpec mysql = normalize("mysql");

        assertThat(mysql.dmlInsertAlternatives())
                .containsExactly("update_on_exists", "ignore_on_exists", "just_insert");
        assertThat(mysql.hasDmlUpdatePolicy()).isTrue();
    }

    @Test
    void reportsNoDeclaredModesWhenTheNamespaceIsAbsent() {
        // The upstream spec has no tapstate namespace yet; null means "derive defaults apply".
        assertThat(normalize("mysql").declaredModes()).isNull();
    }

    // ---- csv: file, protocol disjunction reactions, hidden field ----

    @Test
    void invertsDisjunctionVisibilityReactions() {
        NormalizedSpec csv = normalize("csv");

        assertThat(field(csv, "encoding").visibleWhen())
                .isEqualTo(new VisibleWhen("protocol", List.of("ftp", "sftp")));
        assertThat(field(csv, "ftpHost").visibleWhen())
                .isEqualTo(new VisibleWhen("protocol", List.of("ftp")));
        assertThat(field(csv, "accessKey").visibleWhen())
                .isEqualTo(new VisibleWhen("protocol", List.of("oss", "s3fs")));
    }

    @Test
    void excludesHiddenFields() {
        // loadSchema is x-display:hidden — an internal field, not part of the wizard.
        assertThat(fieldNames(normalize("csv"))).doesNotContain("loadSchema");
    }

    @Test
    void classifiesCsvAsFile() {
        assertThat(normalize("csv").tagGroup()).isEqualTo(ConnectorGroup.FILE);
    }

    // ---- kafka: boolean controller, nested reactions, literal enum labels ----

    @Test
    void invertsBooleanControllerReactionsIncludingNestedOnes() {
        NormalizedSpec kafka = normalize("kafka");

        assertThat(field(kafka, "krb5Keytab").visibleWhen())
                .isEqualTo(new VisibleWhen("krb5", List.of("true")));
        assertThat(field(kafka, "mqUsername").visibleWhen())
                .isEqualTo(new VisibleWhen("krb5", List.of("false")));
        // schemaRegister lives in OPTIONAL_FIELDS and controls a sibling in the same container.
        assertThat(field(kafka, "schemaRegisterUrl").visibleWhen())
                .isEqualTo(new VisibleWhen("schemaRegister", List.of("true")));
    }

    @Test
    void keepsLiteralEnumLabelsUnderTheDefaultLocale() {
        ConfigField mechanism = field(normalize("kafka"), "kafkaSaslMechanism");

        assertThat(mechanism.options()).extracting(EnumOption::value)
                .containsExactly("PLAIN", "SHA256", "SHA512");
        // Literal (non-${}) labels are not localized; they sit under the default locale only.
        assertThat(mechanism.options().get(0).label()).containsExactly(Map.entry("en_US", "PLAIN"));
    }

    // ---- coding: SaaS, capitalized "String" types, void Button container, no capabilities ----

    @Test
    void classifiesCodingAsSaasFromItsTag() {
        assertThat(normalize("coding").tagGroup()).isEqualTo(ConnectorGroup.SAAS);
    }

    @Test
    void foldsACapitalizedFormilyTypeToString() {
        // teamName declares type "String" (capitalized) upstream — it must still normalize to STRING.
        ConfigField teamName = field(normalize("coding"), "teamName");

        assertThat(teamName.type()).isEqualTo(ConfigType.STRING);
        assertThat(teamName.required()).isTrue();
        assertThat(teamName.label())
                .containsEntry("en_US", "Team name").doesNotContainKey("zh_CN");
    }

    @Test
    void dropsTheVoidButtonContainerFromSaasFields() {
        NormalizedSpec coding = normalize("coding");

        assertThat(fieldNames(coding))
                .contains("teamName", "token", "projectName", "connectionMode", "streamReadType");
        // hookButton is a void Button — a layout control, not an answerable field.
        assertThat(fieldNames(coding)).doesNotContain("hookButton");
    }

    @Test
    void reportsNoSinkSignalsOrDeclaredModesForACapabilitylessSaasConnector() {
        NormalizedSpec coding = normalize("coding");

        assertThat(coding.dmlInsertAlternatives()).isEmpty();
        assertThat(coding.hasDmlUpdatePolicy()).isFalse();
        assertThat(coding.declaredModes()).isNull();
    }

    // ---- hand-built trees: declared modes + data defects ----

    @Test
    void readsDeclaredModesFromTheTapstateNamespace() {
        Map<String, Object> tree = Map.of(
                "properties", Map.of("id", "kafka", "name", "Kafka"),
                "tapstate", Map.of("modes", List.of("stream")));

        assertThat(normalize(tree).declaredModes()).containsExactly("stream");
    }

    @Test
    void reducesBooleanInequalityReactionsToTheVisibleValue() {
        Map<String, Object> tree = Map.of(
                "properties", Map.of("id", "mongodb"),
                "configOptions", Map.of("connection", Map.of("properties", Map.of(
                        "isUri", Map.of(
                                "type", "boolean",
                                "x-reactions", List.of(
                                        Map.of("target", "*(uri)", "fulfill", Map.of(
                                                "state", Map.of("visible", "{{$self.value!==false}}"))),
                                        Map.of("target", "*(host)", "fulfill", Map.of(
                                                "state", Map.of("visible", "{{$self.value===false}}"))))),
                        "uri", Map.of("type", "string", "required", true),
                        "host", Map.of("type", "string", "required", true)))));

        assertThat(field(normalize(tree), "uri").visibleWhen())
                .isEqualTo(new VisibleWhen("isUri", List.of("true")));
        assertThat(field(normalize(tree), "host").visibleWhen())
                .isEqualTo(new VisibleWhen("isUri", List.of("false")));
    }

    @Test
    void dropsMixedEqualityAndInequalityVisibilityConditions() {
        Map<String, Object> tree = Map.of(
                "properties", Map.of("id", "mixed"),
                "configOptions", Map.of("connection", Map.of("properties", Map.of(
                        "isUri", Map.of(
                                "type", "boolean",
                                "x-reactions", List.of(Map.of(
                                        "target", "*(uri)",
                                        "fulfill", Map.of("state", Map.of(
                                                "visible", "{{$self.value===false || $self.value!==true}}"))))),
                        "uri", Map.of("type", "string", "required", true)))));

        assertThat(field(normalize(tree), "uri").visibleWhen()).isNull();
    }

    @Test
    void rejectsASpecWithoutAnId() {
        Map<String, Object> tree = Map.of("properties", Map.of("name", "Nameless"));

        assertThatThrownBy(() -> normalize(tree)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void foldsTypeSynonymsAndCasingToConfigType() {
        Map<String, Object> tree = Map.of(
                "properties", Map.of("id", "x"),
                "configOptions", Map.of("connection", Map.of("properties", Map.of(
                        "a", Map.of("type", "String"),
                        "b", Map.of("type", "int"),
                        "c", Map.of("type", "INTEGER")))));

        NormalizedSpec spec = normalize(tree);
        assertThat(field(spec, "a").type()).isEqualTo(ConfigType.STRING);
        assertThat(field(spec, "b").type()).isEqualTo(ConfigType.NUMBER);
        assertThat(field(spec, "c").type()).isEqualTo(ConfigType.NUMBER);
    }

    @Test
    void keepsOnlyTheEnglishLabelAndDropsOtherLocales() {
        // tapstate ships English-only labels: a connector-supplied non-en locale is dropped, en_US kept.
        Map<String, Object> tree = Map.of(
                "properties", Map.of("id", "x"),
                "messages", Map.of(
                        "en_US", Map.of("host", "Host"),
                        "xx_XX", Map.of("host", "Host-in-another-locale")),
                "configOptions", Map.of("connection", Map.of("properties", Map.of(
                        "host", Map.of("type", "string", "title", "${host}")))));

        assertThat(field(normalize(tree), "host").label())
                .containsEntry("en_US", "Host")
                .doesNotContainKey("xx_XX");
    }

    @Test
    void serializesAStructuredDefaultAsCompactJsonNotJavaToString() {
        Map<String, Object> tree = Map.of(
                "properties", Map.of("id", "tdengine"),
                "configOptions", Map.of("connection", Map.of("properties", Map.of(
                        "loadTableOptions", Map.of(
                                "type", "array",
                                "default", List.of("NORMAL_TABLE", "SUPER_TABLE", "CHILD_TABLE"))))));

        ConfigField field = field(normalize(tree), "loadTableOptions");
        assertThat(field.type()).isEqualTo(ConfigType.ARRAY);
        assertThat(field.defaultValue())
                .isEqualTo("[\"NORMAL_TABLE\",\"SUPER_TABLE\",\"CHILD_TABLE\"]");
    }

    @Test
    void skipsANonObjectPropertyValueInsteadOfEmittingAnEmptyField() {
        Map<String, Object> tree = Map.of(
                "properties", Map.of("id", "x"),
                "configOptions", Map.of("connection", Map.of("properties", Map.of(
                        "host", Map.of("type", "string"),
                        "weird", "notAnObject"))));

        assertThat(fieldNames(normalize(tree))).containsExactly("host");
    }

    // ---- helpers ----

    private static NormalizedSpec normalize(String fixture) {
        return SpecNormalizer.normalize(load(fixture));
    }

    private static NormalizedSpec normalize(Map<String, Object> tree) {
        return SpecNormalizer.normalize(tree);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> load(String fixture) {
        String resource = "/specs/" + fixture + ".json";
        try (InputStream in = SpecNormalizerTest.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("missing fixture " + resource);
            }
            return new ObjectMapper().readValue(in, Map.class);
        } catch (Exception e) {
            throw new IllegalStateException("loading fixture " + resource, e);
        }
    }

    private static List<String> fieldNames(NormalizedSpec spec) {
        return spec.config().stream().map(ConfigField::name).toList();
    }

    private static ConfigField field(NormalizedSpec spec, String name) {
        return spec.config().stream()
                .filter(f -> f.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no field named " + name + " in " + fieldNames(spec)));
    }
}
