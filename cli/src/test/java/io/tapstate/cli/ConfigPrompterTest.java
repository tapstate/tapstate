package io.tapstate.cli;

import io.tapstate.core.catalog.ConfigField;
import io.tapstate.core.catalog.ConfigType;
import io.tapstate.core.catalog.EnumOption;
import io.tapstate.core.catalog.VisibleWhen;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The connector config Q&A, driven by synthetic field descriptors (decoupled from the live catalog
 * so the tests stay stable as connector specs evolve). Covers the value rules the wizard must honour:
 * type-aware coercion, enum choices, masked secrets, conditional visibility, and blank = omit.
 */
class ConfigPrompterTest {

    private static ConfigField field(String name, ConfigType type, boolean secret,
                                     List<EnumOption> options, VisibleWhen visibleWhen) {
        return new ConfigField(name, type, Map.of("en_US", name), false, null, secret, options, visibleWhen);
    }

    private static EnumOption opt(String value) {
        return new EnumOption(value, Map.of("en_US", value));
    }

    @Test
    void asksStringFieldsIncludingAnsweredOmittingBlank() {
        List<ConfigField> fields = List.of(
                field("host", ConfigType.STRING, false, List.of(), null),
                field("schema", ConfigType.STRING, false, List.of(), null));
        ScriptedPrompter p = new ScriptedPrompter("10.0.0.1", "");

        Map<String, Object> cfg = new ConfigPrompter().collect(fields, p);

        assertThat(cfg).hasSize(1).containsEntry("host", "10.0.0.1");
    }

    @Test
    void enumFieldOffersValuesPlusSkipAndRecordsTheChoice() {
        List<ConfigField> fields = List.of(
                field("deploymentMode", ConfigType.STRING, false, List.of(opt("standalone"), opt("master-slave")), null));
        ScriptedPrompter p = new ScriptedPrompter("standalone");

        Map<String, Object> cfg = new ConfigPrompter().collect(fields, p);

        assertThat(cfg).containsEntry("deploymentMode", "standalone");
        assertThat(p.offered.get(0)).containsExactly("standalone", "master-slave", "(skip)");
    }

    @Test
    void enumFieldIsOmittedWhenSkipChosen() {
        List<ConfigField> fields = List.of(
                field("deploymentMode", ConfigType.STRING, false, List.of(opt("standalone")), null));
        ScriptedPrompter p = new ScriptedPrompter("(skip)");

        assertThat(new ConfigPrompter().collect(fields, p)).isEmpty();
    }

    @Test
    void conditionalFieldIsAskedOnlyWhenItsControllerMatches() {
        ConfigField deploy = field("deploymentMode", ConfigType.STRING, false,
                List.of(opt("standalone"), opt("uri")), null);
        ConfigField gated = field("authType", ConfigType.STRING, false, List.of(opt("password")),
                new VisibleWhen("deploymentMode", List.of("standalone")));
        List<ConfigField> fields = List.of(deploy, gated);

        // controller answered "uri" -> the gated enum is never offered
        ScriptedPrompter hidden = new ScriptedPrompter("uri");
        assertThat(new ConfigPrompter().collect(fields, hidden)).doesNotContainKey("authType");
        assertThat(hidden.offered).hasSize(1);

        // controller answered "standalone" -> the gated enum is offered
        ScriptedPrompter shown = new ScriptedPrompter("standalone", "password");
        Map<String, Object> cfg = new ConfigPrompter().collect(fields, shown);
        assertThat(cfg).containsEntry("authType", "password");
        assertThat(shown.offered).hasSize(2);
    }

    @Test
    void coercesNumberAndBooleanAnswersToTypedValues() {
        List<ConfigField> fields = List.of(
                field("port", ConfigType.NUMBER, false, List.of(), null),
                field("ssl", ConfigType.BOOLEAN, false, List.of(), null));
        ScriptedPrompter p = new ScriptedPrompter("1521", "true");

        Map<String, Object> cfg = new ConfigPrompter().collect(fields, p);

        assertThat(cfg).containsEntry("port", 1521).containsEntry("ssl", true);
    }

    @Test
    void secretFieldsUseTheMaskedPrompt() {
        List<ConfigField> fields = List.of(field("password", ConfigType.STRING, true, List.of(), null));
        ScriptedPrompter p = new ScriptedPrompter("s3cr3t");

        Map<String, Object> cfg = new ConfigPrompter().collect(fields, p);

        assertThat(cfg).containsEntry("password", "s3cr3t");
        assertThat(p.secretQuestions).hasSize(1);
    }

    /**
     * The essentials-only walk the guided first run takes. The host sits behind an optional deployment
     * mode that is never asked, so its gate is judged by that field's default; the required port takes
     * its default on an empty reply; the optional field is neither asked nor written; the secret is
     * asked masked. A flag answer wins over the prompt, and the walk with no prompter at all lands on
     * the same map - which is what lets the scripted form and the interactive one agree byte for byte.
     */
    @Test
    void essentialWalkAsksRequiredAndSecretFieldsOnlyAndReadsGatesByDefault() {
        ConfigField mode = new ConfigField("deploymentMode", ConfigType.STRING, Map.of("en_US", "mode"), false,
                "standalone", false, List.of(opt("standalone"), opt("cluster")), null);
        ConfigField host = new ConfigField("host", ConfigType.STRING, Map.of("en_US", "host"), true, null, false,
                List.of(), new VisibleWhen("deploymentMode", List.of("standalone")));
        ConfigField port = new ConfigField("port", ConfigType.NUMBER, Map.of("en_US", "port"), true, "3306", false,
                List.of(), null);
        ConfigField extra = new ConfigField("charset", ConfigType.STRING, Map.of("en_US", "charset"), false, "utf8",
                false, List.of(), null);
        ConfigField secret = new ConfigField("password", ConfigType.STRING, Map.of("en_US", "password"), false, null,
                true, List.of(), null);
        List<ConfigField> fields = List.of(mode, host, port, extra, secret);

        ScriptedPrompter asked = new ScriptedPrompter("db", "", "s");
        Map<String, Object> interactive = new ConfigPrompter().collectEssential(fields, Map.of(), asked);

        assertThat(interactive).containsExactly(
                Map.entry("host", "db"), Map.entry("port", 3306), Map.entry("password", "s"));
        assertThat(asked.asked).containsExactly("host", "port");
        assertThat(asked.secretQuestions).containsExactly("password");
        assertThat(asked.offered).as("the optional mode is never offered").isEmpty();

        Map<String, String> given = Map.of("host", "db", "password", "s");
        assertThat(new ConfigPrompter().collectEssential(fields, given, null)).isEqualTo(interactive);
        ScriptedPrompter notAsked = new ScriptedPrompter();
        assertThat(new ConfigPrompter().collectEssential(fields, given, notAsked)).isEqualTo(interactive);
        assertThat(notAsked.asked).as("a flag answer is not asked again; only the port's default is taken")
                .containsExactly("port");
    }

    @Test
    void essentialWalkUsesASuppliedControllerBeforeItsDefault() {
        ConfigField mode = new ConfigField("deploymentMode", ConfigType.STRING, Map.of("en_US", "mode"), false,
                "standalone", false, List.of(opt("standalone"), opt("cluster")), null);
        ConfigField host = new ConfigField("host", ConfigType.STRING, Map.of("en_US", "host"), true, null, false,
                List.of(), new VisibleWhen("deploymentMode", List.of("standalone")));
        ConfigField secret = new ConfigField("password", ConfigType.STRING, Map.of("en_US", "password"), false,
                null, true, List.of(), null);

        ScriptedPrompter prompter = new ScriptedPrompter("s");
        Map<String, Object> config = new ConfigPrompter().collectEssential(
                List.of(mode, host, secret), Map.of("deploymentMode", "cluster"), prompter);

        assertThat(config).containsExactly(Map.entry("password", "s"), Map.entry("deploymentMode", "cluster"));
        assertThat(prompter.asked).isEmpty();
        assertThat(prompter.secretQuestions).containsExactly("password");
    }

    @Test
    void essentialWalkTreatsNullControllerDefaultAsHidden() {
        ConfigField controller = new ConfigField("sslValidate", ConfigType.BOOLEAN,
                Map.of("en_US", "sslValidate"), false, null, false, List.of(), null);
        ConfigField gated = new ConfigField("sslCa", ConfigType.STRING,
                Map.of("en_US", "sslCa"), true, null, false, List.of(),
                new VisibleWhen("sslValidate", List.of("true")));

        Map<String, Object> config = new ConfigPrompter().collectEssential(
                List.of(controller, gated), Map.of(), new ScriptedPrompter("unused"));

        assertThat(config).isEmpty();
    }
}
