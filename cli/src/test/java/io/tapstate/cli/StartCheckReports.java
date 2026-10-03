package io.tapstate.cli;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Start check reports and start answers in the wire shape a server sends them, for the cases that drive the
 * command line through its start checks. Built as maps and read by the parser the HTTP client uses, so a case
 * exercises what arrives rather than a shape assembled for the test.
 */
final class StartCheckReports {

    static final String HASH = "1f0e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0";
    static final String NEEDS_CONFIRMATION = "lifecycle.start-needs-confirmation";

    @SafeVarargs
    static Map<String, Object> report(String outcome, Map<String, Object>... findings) {
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("pipelineId", "pl1");
        report.put("intent", "START");
        report.put("contentHash", HASH);
        report.put("evaluatedAt", "2026-10-02T08:00:00Z");
        report.put("outcome", outcome);
        report.put("plan", List.of(Map.of("element", "orders",
                "target", Map.of("connection", "warehouse", "table", "orders"),
                "load", "FULL_LOAD", "onFullLoad", "append")));
        report.put("findings", List.of(findings));
        return report;
    }

    static Map<String, Object> withHash(Map<String, Object> report, String hash) {
        report.put("contentHash", hash);
        return report;
    }

    static Map<String, Object> withIntent(Map<String, Object> report, String intent) {
        report.put("intent", intent);
        return report;
    }

    /** A target that already holds rows, with both answers: clear it, or keep the rows. */
    static Map<String, Object> notEmpty(String table) {
        return finding(table, "CONFIRM", table + " on warehouse already holds 5 rows.", List.of(
                action("clear", "CHANGE_DEFINITION", true, "Clear " + table + " before the full load",
                        List.of(Map.of("element", table, "field", "on_full_load", "from", "append", "to", "clear"))),
                action("keep", "ACKNOWLEDGE", false, "Keep the rows already in " + table, List.of())));
    }

    /** The same on a target a shared definition declares, which this pipeline cannot change: keep only. */
    static Map<String, Object> notEmptyShared(String table) {
        return finding(table, "CONFIRM", table + " on warehouse already holds 5 rows; a shared definition "
                + "declares it.", List.of(
                action("keep", "ACKNOWLEDGE", false, "Keep the rows already in " + table, List.of())));
    }

    static Map<String, Object> blocked(String table) {
        return finding(table, "BLOCK", table + " on warehouse already holds 5 rows, and on_full_load is fail.",
                List.of());
    }

    static Map<String, Object> passed(String table) {
        return finding(table, "PASS", table + " on warehouse is empty.", List.of());
    }

    static Map<String, Object> warning(String check, String message) {
        Map<String, Object> finding = new LinkedHashMap<>();
        finding.put("key", check + "/src_pg");
        finding.put("check", check);
        finding.put("subject", Map.of("kind", "SOURCE", "id", "src_pg", "label", "source src_pg"));
        finding.put("behavior", "WARN");
        finding.put("evaluation", "COMPLETE");
        finding.put("message", message);
        finding.put("actions", List.of());
        return finding;
    }

    static Map<String, Object> finding(String table, String behavior, String message,
            List<Map<String, Object>> actions) {
        Map<String, Object> finding = new LinkedHashMap<>();
        finding.put("key", "target-not-empty/warehouse/" + table);
        finding.put("check", "target-not-empty");
        finding.put("subject", Map.of("kind", "TARGET", "id", "warehouse/" + table, "element", table,
                "label", table + " on warehouse"));
        finding.put("behavior", behavior);
        finding.put("evaluation", "COMPLETE");
        finding.put("message", message);
        finding.put("actions", actions);
        return finding;
    }

    static Map<String, Object> action(String id, String kind, boolean destructive, String message,
            List<Map<String, Object>> changes) {
        Map<String, Object> action = new LinkedHashMap<>();
        action.put("id", id);
        action.put("kind", kind);
        action.put("destructive", destructive);
        action.put("message", message);
        action.put("changes", changes);
        return action;
    }

    static Map<String, Object> applied(String table) {
        return Map.of("finding", "target-not-empty/warehouse/" + table, "action", "clear",
                "changes", List.of(Map.of("element", table, "field", "on_full_load", "from", "append", "to", "clear")));
    }

    static StartAttempt.Stopped stoppedBy(Map<String, Object> report) {
        return new StartAttempt.Stopped(NEEDS_CONFIRMATION, Map.of("pipeline", "pl1"),
                "pl1 needs an answer before it starts", StartChecks.parse(report));
    }

    static StartAttempt.Started started(Map<String, Object> report, List<Map<String, Object>> applied,
            List<Map<String, Object>> stale) {
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put("pipelineId", "pl1");
        raw.put("targetState", "RUNNING");
        raw.put("revision", "rev-2");
        if (report != null) {
            raw.put("startChecks", report);
            raw.put("decisionsApplied", applied);
            raw.put("staleDecisions", stale);
        }
        return new StartAttempt.Started("pl1", "RUNNING", "rev-2", report == null ? null : StartChecks.parse(report),
                applied, stale, raw);
    }

    private StartCheckReports() {
    }
}
