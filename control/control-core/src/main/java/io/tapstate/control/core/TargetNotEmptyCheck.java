package io.tapstate.control.core;

import io.tapstate.core.model.OnFullLoad;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.spi.store.StartLoad;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Asks before a new full load goes into a target that already holds rows and will not be cleared first.
 *
 * <p>A new full load copies the rows the source holds now. Rows already in the target that the source no
 * longer holds -- deleted while the pipeline was stopped -- are left where they are by a full load that
 * keeps existing rows, and nothing afterwards ever removes them: the target silently stops matching its
 * source. So when a start would do exactly that, it stops and asks, and offers to clear the target first
 * and to keep doing so on every later full load, by setting that on the element that writes it.
 *
 * <p>What the target's own policy says decides the rest. A target set to be cleared passes. A target set
 * to refuse a non-empty start refuses this one now, rather than after the start has been accepted. A
 * target that holds nothing passes. A target that cannot be looked at is said to be so, and the start goes
 * ahead without asking.
 *
 * <p>The answer that changes the definition is offered only for an element written in the pipeline
 * itself. An element that comes from a shared definition is shared with other pipelines, and a start of
 * one of them is no place to change all of them; the question then names that definition instead.
 */
public final class TargetNotEmptyCheck implements StartCheck {

    public static final String ID = "target-not-empty";
    static final String CLEAR = "clear";
    static final String KEEP = "keep";

    @Override
    public String id() {
        return ID;
    }

    @Override
    public List<StartFinding> evaluate(StartCheckContext context) {
        List<StartPlan.Entry> fullLoads = context.plan().entries().stream()
                .filter(entry -> entry.load() == StartLoad.FULL_LOAD)
                .toList();
        if (fullLoads.isEmpty()) {
            return List.of();
        }
        // One finding per target, however many elements write it, so that one answer covers it.
        Map<String, List<StartPlan.Entry>> byTarget = new LinkedHashMap<>();
        for (StartPlan.Entry entry : fullLoads) {
            byTarget.computeIfAbsent(entry.coordinate(), coordinate -> new ArrayList<>()).add(entry);
        }
        Map<String, StartCheckContext.Probed> probed =
                context.probe(byTarget.values().stream().map(List::getFirst).toList());
        List<StartFinding> findings = new ArrayList<>();
        byTarget.forEach((coordinate, writers) -> findings.add(finding(writers, probed.get(coordinate))));
        return findings;
    }

    private StartFinding finding(List<StartPlan.Entry> writers, StartCheckContext.Probed probed) {
        StartPlan.Entry first = writers.getFirst();
        PipelineWriteTargets.WriteTarget target = first.target();
        StartFinding.Subject subject = new StartFinding.Subject(
                StartFinding.Subject.TARGET, first.coordinate(), target.element(), label(target));
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("target", target.table());
        params.put("connection", target.connection());
        params.put("element", target.element());
        params.put("onFullLoad", policy(writers).yaml());

        if (probed instanceof StartCheckContext.Probed.Unknown unknown) {
            params.put("reason", unknown.reason());
            if (unknown.code() != null) {
                params.put("reasonCode", unknown.code());
            }
            return new StartFinding(ID, subject, whenUnavailable(), StartFinding.Evaluation.UNAVAILABLE,
                    StartCheckCode.TARGET_ROWS_UNKNOWN, params, List.of());
        }
        TargetProbe.TargetRows rows = ((StartCheckContext.Probed.Rows) probed).rows();
        if (rows.empty()) {
            params.put("rows", 0L);
            params.put("rowsExact", true);
            return complete(subject, StartFinding.Behavior.PASS, StartCheckCode.TARGET_EMPTY, params, List.of());
        }
        params.put("rows", rows.count());
        params.put("rowsExact", rows.count() != null && rows.countExact());
        params.put("holding", holding(rows));
        return switch (policy(writers)) {
            case CLEAR -> complete(subject, StartFinding.Behavior.PASS, StartCheckCode.TARGET_SET_TO_CLEAR,
                    params, List.of());
            case FAIL -> complete(subject, StartFinding.Behavior.BLOCK, StartCheckCode.TARGET_NOT_EMPTY_REFUSED,
                    params, List.of());
            case APPEND -> confirm(subject, writers, params);
        };
    }

    private StartFinding confirm(
            StartFinding.Subject subject, List<StartPlan.Entry> writers, Map<String, Object> params) {
        StartAction keep = StartAction.acknowledge(KEEP, StartCheckCode.KEEP_EXISTING_ROWS,
                Map.of("target", params.get("target")));
        String shared = writers.stream().map(entry -> entry.target().definedIn())
                .filter(Objects::nonNull).findFirst().orElse(null);
        if (shared != null) {
            params.put("definition", shared);
            return complete(subject, StartFinding.Behavior.CONFIRM, StartCheckCode.TARGET_NOT_EMPTY_SHARED,
                    params, List.of(keep));
        }
        List<String> elements = writers.stream().map(entry -> entry.target().element()).distinct().toList();
        List<StartAction.Change> changes = elements.stream()
                .map(element -> new StartAction.Change(element, "on_full_load",
                        OnFullLoad.APPEND.yaml(), OnFullLoad.CLEAR.yaml()))
                .toList();
        StartAction clear = StartAction.changeDefinition(CLEAR, true, StartCheckCode.CLEAR_BEFORE_FULL_LOAD,
                Map.of("target", params.get("target"), "element", String.join(", ", elements)),
                changes, definition -> clearedFirst(definition, elements));
        return complete(subject, StartFinding.Behavior.CONFIRM, StartCheckCode.TARGET_NOT_EMPTY,
                params, List.of(clear, keep));
    }

    private static StartFinding complete(StartFinding.Subject subject, StartFinding.Behavior behavior,
            StartCheckCode code, Map<String, Object> params, List<StartAction> actions) {
        return new StartFinding(ID, subject, behavior, StartFinding.Evaluation.COMPLETE, code, params, actions);
    }

    /**
     * The policy a full load applies to the target: one element's own, or -- when several elements write
     * the same target -- the one that keeps the most, because a target is cleared only when every element
     * writing it would clear it.
     */
    private static OnFullLoad policy(List<StartPlan.Entry> writers) {
        boolean fails = writers.stream().anyMatch(entry -> entry.onFullLoad() == OnFullLoad.FAIL);
        if (fails) {
            return OnFullLoad.FAIL;
        }
        return writers.stream().allMatch(entry -> entry.onFullLoad() == OnFullLoad.CLEAR)
                ? OnFullLoad.CLEAR : OnFullLoad.APPEND;
    }

    /** {@code definition} with every named element set to clear its target before a new full load. */
    static PipelineResource clearedFirst(PipelineResource definition, List<String> elements) {
        ServeBlock serve = definition.serve();
        if (serve instanceof ServeBlock.Inline inline && inline.sync() != null) {
            List<SyncElement> sync = inline.sync().stream()
                    .map(element -> elements.contains(elementId(element))
                            ? new SyncElement(element.id(), element.source(), element.writeMode(), element.rename(),
                                    element.ddl(), OnFullLoad.CLEAR)
                            : element)
                    .toList();
            serve = new ServeBlock.Inline(inline.id(), inline.from(), sync, inline.query(), inline.push());
        }
        ViewBlock view = definition.view();
        if (view instanceof ViewBlock.Inline inline && elements.contains(inline.id())) {
            view = new ViewBlock.Inline(inline.id(), inline.from(), inline.primaryKey(), inline.storage(),
                    inline.writeMode(), OnFullLoad.CLEAR);
        }
        return new PipelineResource(definition.id(), definition.metadata(), definition.sources(),
                definition.transforms(), view, serve, definition.settings(), definition.experimental());
    }

    /** How a sink names a sync element: its id, or the source it writes to when it has none. */
    private static String elementId(SyncElement element) {
        return element.id() != null && !element.id().isBlank() ? element.id() : element.source();
    }

    private static String label(PipelineWriteTargets.WriteTarget target) {
        return target.kind() == PipelineWriteTargets.WriteTarget.Kind.VIEW
                ? "view " + target.element() + " (" + target.connection() + ")"
                : target.table() + " on " + target.connection();
    }

    /** How much a target holds, as the finding's sentence says it. */
    private static String holding(TargetProbe.TargetRows rows) {
        if (rows.count() == null || rows.count() < 1) {
            return "at least one row";
        }
        return rows.count() == 1 ? "1 row" : rows.count() + " rows";
    }
}
