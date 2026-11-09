package io.tapstate.messages;

import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every nest refusal, and every refusal of the nest commands, says what to do next - not only what went
 * wrong. A solution that explains the cause and stops there leaves the reader with a diagnosis and no move;
 * this holds each one to carrying at least one clause that tells the reader to do something.
 *
 * <p>"Tells the reader to do something" is read as a clause that opens with one of the verbs below: at the
 * start of the text, or after a sentence end, a semicolon, a dash or a comma. A new code whose solution has
 * none fails here, and the fix is to write the action, not to add a verb to the list.
 */
class EveryNestRefusalNamesTheNextActionTest {

    private static final Pattern ACTION = Pattern.compile(
            "(?:^|[.;:,]\\s+|\\s-\\s+|\\bor\\s+|\\bthen\\s+)(?:ask|change|check|configure|declare|discover|"
                    + "embed|find|flatten|give|have|join|leave|let|make|move|name|narrow|raise|rename|restart|"
                    + "reverse|run|split|start|stop|use|accept|drop|migrate)\\b",
            Pattern.CASE_INSENSITIVE);

    @Test
    void everyNestSolutionCarriesAnAction() throws Exception {
        Map<String, Object> catalog;
        try (InputStream in = getClass().getResourceAsStream("/messages/en.yml")) {
            catalog = new Yaml().load(in);
        }
        List<String> nest = new ArrayList<>();
        List<String> withoutAction = new ArrayList<>();
        catalog.forEach((code, entry) -> {
            if (!code.startsWith("nest.") && !code.startsWith("dsl.nest") && !code.startsWith("monitor.nest")
                    && !code.equals("monitor.no-nest-step") && !code.equals("monitor.unknown-nest-step")) {
                return;
            }
            nest.add(code);
            Object solution = entry instanceof Map<?, ?> fields ? fields.get("solution") : null;
            if (!(solution instanceof String text) || !ACTION.matcher(text).find()) {
                withoutAction.add(code);
            }
        });

        assertThat(nest).as("the nest codes this reads").hasSizeGreaterThan(25);
        assertThat(withoutAction).as("nest codes whose solution names no next action").isEmpty();
    }
}
