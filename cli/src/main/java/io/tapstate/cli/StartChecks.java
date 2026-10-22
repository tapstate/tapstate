package io.tapstate.cli;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A start check report as the server sent it: kept whole for the structured output, and read for what
 * this client needs to show it and answer it.
 *
 * <p>Read the way every client has to read it. Texts are shown as they arrive; a kind, a load or an
 * evaluation this client does not know is shown as written; and anything that decides whether a start may
 * go ahead fails closed -- a behavior this client does not know is a refusal, and so is an outcome it does
 * not know. The one thing this client recognises by name is the answer "go ahead as configured", which
 * every question offers exactly once and which is what {@code -y} chooses.
 *
 * @param raw         the report as it arrived, for {@code -o json|yaml}
 * @param contentHash the definition the report was evaluated against, which an answer is sent back under
 * @param findings    every finding, in the order the server listed them
 */
record StartChecks(Map<String, Object> raw, String pipelineId, String intent, String contentHash, String outcome,
        List<Load> plan, List<Finding> findings) {

    static final String PASS = "PASS";
    static final String WARN = "WARN";
    static final String CONFIRM = "CONFIRM";
    static final String BLOCK = "BLOCK";
    private static final Set<String> KNOWN_BEHAVIORS = Set.of(PASS, WARN, CONFIRM, BLOCK);
    private static final Set<String> KNOWN_OUTCOMES = Set.of("READY", "NEEDS_CONFIRMATION", "BLOCKED");
    static final String ACKNOWLEDGE = "ACKNOWLEDGE";

    StartChecks {
        raw = Collections.unmodifiableMap(new LinkedHashMap<>(raw));
        plan = List.copyOf(plan);
        findings = List.copyOf(findings);
    }

    /** How the start loads one target, as the report says it. */
    record Load(String element, String connection, String table, String load, String onFullLoad) {
    }

    /** One finding. {@code behavior} is the server's word; {@link #refuses()} is how this client reads it. */
    record Finding(String key, String check, String behavior, String evaluation, String subject, String message,
            List<Action> actions) {

        Finding {
            actions = List.copyOf(actions);
        }

        /** Whether this finding stops the start whatever is answered, an unknown behavior included. */
        boolean refuses() {
            return BLOCK.equals(behavior) || !KNOWN_BEHAVIORS.contains(behavior);
        }

        boolean asks() {
            return CONFIRM.equals(behavior);
        }

        /** The answer that goes ahead as configured. */
        Action acknowledgement() {
            return actions.stream().filter(action -> ACKNOWLEDGE.equals(action.kind())).findFirst().orElse(null);
        }

        Action action(String id) {
            return actions.stream().filter(action -> action.id().equals(id)).findFirst().orElse(null);
        }
    }

    /** One answer a finding offers. */
    record Action(String id, String kind, boolean destructive, String message, List<Change> changes) {

        Action {
            changes = List.copyOf(changes);
        }
    }

    /** One field an answer changes in the definition. */
    record Change(String element, String field, String from, String to) {
    }

    /** Whether the start cannot go ahead whatever is answered. */
    boolean refused() {
        return !KNOWN_OUTCOMES.contains(outcome) || "BLOCKED".equals(outcome)
                || findings.stream().anyMatch(Finding::refuses);
    }

    /** The questions, in order. */
    List<Finding> questions() {
        return findings.stream().filter(Finding::asks).toList();
    }

    /** The finding under {@code key}, or null. */
    Finding finding(String key) {
        return findings.stream().filter(finding -> finding.key().equals(key)).findFirst().orElse(null);
    }

    /** Whether the start runs a new full load into any target. */
    boolean loadsAfresh() {
        return plan.stream().anyMatch(entry -> "FULL_LOAD".equals(entry.load()));
    }

    /** The report in {@code json}, or null when it is not one this client can read. */
    @SuppressWarnings("unchecked")
    static StartChecks parse(Object json) {
        if (!(json instanceof Map<?, ?> map)
                || !(map.get("pipelineId") instanceof String pipelineId)
                || !(map.get("contentHash") instanceof String contentHash)
                || !(map.get("outcome") instanceof String outcome)
                || !(map.get("findings") instanceof List<?> findings)) {
            return null;
        }
        List<Load> plan = new ArrayList<>();
        if (map.get("plan") instanceof List<?> entries) {
            for (Object entry : entries) {
                if (entry instanceof Map<?, ?> e) {
                    Map<?, ?> target = e.get("target") instanceof Map<?, ?> t ? t : Map.of();
                    plan.add(new Load(text(e.get("element")), text(target.get("connection")),
                            text(target.get("table")), text(e.get("load")), text(e.get("onFullLoad"))));
                }
            }
        }
        List<Finding> read = new ArrayList<>();
        for (Object item : findings) {
            if (!(item instanceof Map<?, ?> f) || !(f.get("key") instanceof String key)) {
                return null;
            }
            Map<?, ?> subject = f.get("subject") instanceof Map<?, ?> s ? s : Map.of();
            List<Action> actions = new ArrayList<>();
            if (f.get("actions") instanceof List<?> offered) {
                for (Object candidate : offered) {
                    if (candidate instanceof Map<?, ?> a && a.get("id") instanceof String id) {
                        List<Change> changes = new ArrayList<>();
                        if (a.get("changes") instanceof List<?> listed) {
                            for (Object change : listed) {
                                if (change instanceof Map<?, ?> c) {
                                    changes.add(new Change(text(c.get("element")), text(c.get("field")),
                                            text(c.get("from")), text(c.get("to"))));
                                }
                            }
                        }
                        actions.add(new Action(id, text(a.get("kind")), Boolean.TRUE.equals(a.get("destructive")),
                                text(a.get("message")), changes));
                    }
                }
            }
            String label = text(subject.get("label"));
            read.add(new Finding(key, text(f.get("check")), text(f.get("behavior")), text(f.get("evaluation")),
                    label.isEmpty() ? text(subject.get("id")) : label, text(f.get("message")), actions));
        }
        return new StartChecks((Map<String, Object>) map, pipelineId, text(map.get("intent")), contentHash, outcome,
                plan, read);
    }

    private static String text(Object value) {
        return value == null ? "" : String.valueOf(value);
    }
}
