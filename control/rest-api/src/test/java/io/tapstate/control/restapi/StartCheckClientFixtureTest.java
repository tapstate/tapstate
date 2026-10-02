package io.tapstate.control.restapi;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import io.tapstate.control.core.StartAction;
import io.tapstate.control.core.StartCheckReport;
import io.tapstate.control.core.StartFinding;
import io.tapstate.control.core.StartIntent;
import io.tapstate.spi.store.StartLoad;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The client fixture is a start check report as this server writes it. Clients test that they render and
 * answer reports carrying values they do not know yet against this file, so the file has to keep the shape
 * the server really sends -- every object with exactly the fields the server's own report has -- or those
 * tests prove tolerance of a shape no server produces.
 */
class StartCheckClientFixtureTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void theFixtureIsVersionOneAndCarriesItsThreeReports() throws IOException {
        JsonNode fixture = fixture();
        assertThat(fixture.get("fixtureVersion").asInt()).isEqualTo(1);
        assertThat(names(fixture)).contains("answerable", "unknownBehavior", "unknownOutcome");
    }

    @Test
    void everyReportInTheFixtureHasExactlyTheFieldsTheServerWrites() throws IOException {
        JsonNode written = JSON.valueToTree(serversOwnReport());
        JsonNode fixture = fixture();
        for (String report : List.of("answerable", "unknownBehavior", "unknownOutcome")) {
            assertSameShape(report, written, fixture.get(report));
        }
    }

    private static void assertSameShape(String report, JsonNode written, JsonNode fixture) {
        assertThat(names(fixture)).as("%s: the report's fields", report).isEqualTo(names(written));
        JsonNode writtenFinding = written.get("findings").get(0);
        for (JsonNode finding : fixture.get("findings")) {
            assertThat(names(finding)).as("%s: a finding's fields", report).isEqualTo(names(writtenFinding));
            assertThat(names(finding.get("subject"))).as("%s: a subject's fields", report)
                    .isEqualTo(names(writtenFinding.get("subject")));
            for (JsonNode action : finding.get("actions")) {
                JsonNode writtenAction = writtenFinding.get("actions").get(0);
                assertThat(names(action)).as("%s: an action's fields", report).isEqualTo(names(writtenAction));
                for (JsonNode change : action.get("changes")) {
                    assertThat(names(change)).as("%s: a change's fields", report)
                            .isEqualTo(names(writtenAction.get("changes").get(0)));
                }
            }
        }
        JsonNode writtenEntry = written.get("plan").get(0);
        for (JsonNode entry : fixture.get("plan")) {
            assertThat(names(entry)).as("%s: a plan entry's fields", report).isEqualTo(names(writtenEntry));
            assertThat(names(entry.get("target"))).as("%s: a target's fields", report)
                    .isEqualTo(names(writtenEntry.get("target")));
        }
    }

    private static StartCheckReport serversOwnReport() {
        return new StartCheckReport("lead_view", StartIntent.START, "0".repeat(64), "2026-10-02T08:15:02Z",
                StartCheckReport.Outcome.NEEDS_CONFIRMATION,
                List.of(new StartCheckReport.PlanEntry("lead", new StartCheckReport.Target("views", "lead"),
                        StartLoad.FULL_LOAD, "append")),
                List.of(new StartCheckReport.Finding("target-not-empty/views/lead", "target-not-empty",
                        new StartCheckReport.Subject("TARGET", "views/lead", "lead", "view lead (views)"),
                        StartFinding.Behavior.CONFIRM, StartFinding.Evaluation.COMPLETE,
                        "start-check.target-not-empty", Map.of("rows", 5), "a message",
                        List.of(new StartCheckReport.Action("clear", StartAction.CHANGE_DEFINITION, true,
                                "start-check.clear-before-full-load", "a label",
                                List.of(new StartAction.Change("lead", "on_full_load", "append", "clear")))))));
    }

    private static JsonNode fixture() throws IOException {
        try (InputStream in = StartCheckClientFixtureTest.class.getResourceAsStream(
                "/start-checks/client-fixture-v1.json")) {
            assertThat(in).as("the client fixture on the test classpath").isNotNull();
            return JSON.readTree(in);
        }
    }

    private static Set<String> names(JsonNode node) {
        Set<String> names = new TreeSet<>();
        for (Map.Entry<String, JsonNode> property : node.properties()) {
            names.add(property.getKey());
        }
        return names;
    }
}
