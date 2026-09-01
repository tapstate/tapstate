package io.tapstate.cli;

import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.QueryElement;
import io.tapstate.core.model.QueryType;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The interactive {@code pipeline} flow: ask the pipeline id, the source(s) it reads, an optional
 * chain of transforms, and a serve surface, then build a canonical pipeline resource. References to
 * sources and sink targets are ids of pre-created {@code kind: source} resources — the wizard offers
 * any already present in the workspace and falls back to free-text entry. Transform expressions
 * (CEL) are collected verbatim; {@code validate} compiles them later. It collects answers through a
 * {@link Prompter}, never a terminal directly.
 */
final class PipelineWizard {

    /** The sentinel choice that ends the transform-adding loop. */
    private static final String DONE = "(done)";
    /** The transform-menu choice that reuses an existing {@code kind: transform} definition. */
    private static final String USE = "(use)";
    /** The output-stage choice declining a surface (no serve, or no view). */
    private static final String NONE = "(none)";
    /** The view-menu choice that defines the view inline rather than reusing a definition body. */
    private static final String INLINE = "inline";

    private final Prompter prompter;
    private final TransformBodyPrompter bodyPrompter;
    private final List<String> existingSourceIds;
    private final List<String> reusableTransformIds;
    private final List<String> reusableViewIds;
    private final List<String> reusableServeIds;

    PipelineWizard(Prompter prompter, List<String> existingSourceIds) {
        this(prompter, existingSourceIds, List.of());
    }

    PipelineWizard(Prompter prompter, List<String> existingSourceIds, List<String> reusableTransformIds) {
        this(prompter, existingSourceIds, reusableTransformIds, List.of(), List.of());
    }

    PipelineWizard(Prompter prompter, List<String> existingSourceIds, List<String> reusableTransformIds,
                   List<String> reusableViewIds, List<String> reusableServeIds) {
        this.prompter = prompter;
        this.bodyPrompter = new TransformBodyPrompter(prompter);
        this.existingSourceIds = existingSourceIds;
        this.reusableTransformIds = reusableTransformIds;
        this.reusableViewIds = reusableViewIds;
        this.reusableServeIds = reusableServeIds;
    }

    PipelineResource run() {
        String id = askId("Pipeline id", "pipeline");
        String source = askSourceRef("Source", true);
        List<Step> transforms = askTransforms();
        FromRef output = transforms.isEmpty()
                ? FromRef.regex(".*")
                : FromRef.literal(transforms.get(transforms.size() - 1).id());
        // serve is asked first but finalized last: its from: wires to the view when both are present
        // (natural order transforms -> view -> serve), so collect the choice, then learn the view.
        ServePlan servePlan = askServe();
        ViewBlock view = askView(output, servePlan, reservedIds(transforms, servePlan));
        FromRef serveFrom = view != null ? FromRef.literal(viewId(view)) : output;
        ServeBlock serve = buildServe(servePlan, serveFrom);
        return new PipelineResource(id, null, List.of(source),
                transforms.isEmpty() ? null : transforms, view, serve, null, null);
    }

    /**
     * The pipeline-internal ids already taken when the inline view id is collected: every transform
     * step id, plus the serve block's id ({@code serve} for an inline serve, or the reused name for a
     * use-reference). The inline view shares this flat namespace, so it must avoid these to not produce
     * a duplicate-id artifact that crashes validate.
     */
    private static Set<String> reservedIds(List<Step> transforms, ServePlan servePlan) {
        Set<String> reserved = new HashSet<>();
        for (Step step : transforms) {
            reserved.add(step.id());
        }
        if (servePlan.useId() != null) {
            reserved.add(servePlan.useId());
        } else if (servePlan.present()) {
            reserved.add("serve");
        }
        return reserved;
    }

    /** A serve surface chosen in the serve stage; its elements are collected up front, its from: deferred. */
    private record ServePlan(List<SyncElement> sync, List<QueryElement> query, List<PushElement> push,
                             String useId) {
        boolean present() {
            return sync != null || query != null || push != null || useId != null;
        }
    }

    /** Ask the serve surface: reuse a definition, an inline sync/push/query, or decline it ((none)). */
    private ServePlan askServe() {
        String choice = prompter.choose("Serve?", serveMenu());
        if (reusableServeIds.contains(choice)) {
            return new ServePlan(null, null, null, choice);
        }
        return switch (choice) {
            case "push" -> new ServePlan(null, null, askPush(), null);
            case "query" -> new ServePlan(null, askQuery(), null, null);
            case NONE -> new ServePlan(null, null, null, null);
            default -> new ServePlan(askSync(), null, null, null);   // "sync" or a skipped (exhausted) prompt
        };
    }

    /** Reusable serve definitions first, then the inline surfaces; {@code sync} is last = the safe default. */
    private List<String> serveMenu() {
        List<String> menu = new ArrayList<>(reusableServeIds);
        menu.add("push");
        menu.add("query");
        menu.add(NONE);
        menu.add("sync");
        return menu;
    }

    private ServeBlock buildServe(ServePlan plan, FromRef from) {
        if (!plan.present()) {
            return null;
        }
        if (plan.useId() != null) {
            return new ServeBlock.Use(null, plan.useId(), from);
        }
        return new ServeBlock.Inline("serve", from, plan.sync(), plan.query(), plan.push());
    }

    private List<SyncElement> askSync() {
        String sink = askSourceRef("Sync to (target source id)", false);
        return List.of(new SyncElement("sync_1", sink, null, null, null));
    }

    private List<PushElement> askPush() {
        String sink = askSourceRef("Push to (target source id)", false);
        String topic = WizardPrompts.blankToNull(prompter.ask("Topic (blank for none)", null));
        return List.of(new PushElement("push_1", sink, topic, null));
    }

    private List<QueryElement> askQuery() {
        String type = prompter.choose("Query type?", List.of("graphql", "mcp", "rest"));
        return List.of(new QueryElement(queryType(type), null));
    }

    /**
     * Ask the view surface: reuse a definition, define one inline, or — only when a serve already
     * carries the pipeline output — decline it ((none)). When serve is absent the view is the sole
     * output, so the menu omits (none) and a view is always produced.
     */
    private ViewBlock askView(FromRef from, ServePlan servePlan, Set<String> reserved) {
        String choice = prompter.choose("View?", viewMenu(servePlan.present()));
        if (reusableViewIds.contains(choice)) {
            return new ViewBlock.Use(null, choice, from);
        }
        if (NONE.equals(choice)) {
            return null;
        }
        return askInlineView(from, reserved);
    }

    /** Reusable view definitions first, then {@code inline}; (none) is appended only when serve is present. */
    private List<String> viewMenu(boolean servePresent) {
        List<String> menu = new ArrayList<>(reusableViewIds);
        menu.add(INLINE);
        if (servePresent) {
            menu.add(NONE);
        }
        return menu;
    }

    private ViewBlock askInlineView(FromRef from, Set<String> reserved) {
        // the suggested fallback is dot-free and not in the reserved set, so a skipped/exhausted
        // prompt always settles on a valid distinct id and the re-prompt loop terminates
        String viewId = askId("View id", freshId("view", reserved), reserved);
        String primaryKey = WizardPrompts.askPrimaryKey(prompter);
        return new ViewBlock.Inline(viewId, from, primaryKey, null, null);
    }

    /** {@code base}, else {@code base_2}, {@code base_3}, … — the first id not already in {@code taken}. */
    private static String freshId(String base, Set<String> taken) {
        if (!taken.contains(base)) {
            return base;
        }
        for (int n = 2; ; n++) {
            String candidate = base + "_" + n;
            if (!taken.contains(candidate)) {
                return candidate;
            }
        }
    }

    private static String viewId(ViewBlock view) {
        return switch (view) {
            case ViewBlock.Inline v -> v.id();
            case ViewBlock.Use u -> u.id();
        };
    }

    private static QueryType queryType(String yaml) {
        for (QueryType type : QueryType.values()) {
            if (type.yaml().equals(yaml)) {
                return type;
            }
        }
        throw new IllegalStateException("unhandled query type: " + yaml);
    }

    private List<Step> askTransforms() {
        List<Step> steps = new ArrayList<>();
        while (true) {
            String type = prompter.choose("Add a transform?", transformMenu());
            if (DONE.equals(type)) {
                return steps;
            }
            String prevId = steps.isEmpty() ? null : steps.get(steps.size() - 1).id();
            if (USE.equals(type)) {
                String def = prompter.choose("Reuse which transform?", reusableTransformIds);
                steps.add(Step.use(null, def, askFlow(prevId)));
            } else {
                String id = askId("Transform id", type + "_" + (steps.size() + 1));
                FromClause from = askFlow(prevId);
                TransformBody body = bodyPrompter.askBody(type);
                if (body != null) {
                    steps.add(Step.inline(id, from, body, null));
                }
            }
        }
    }

    private List<String> transformMenu() {
        List<String> menu = new ArrayList<>(List.of("filter", "map", "js"));
        if (!reusableTransformIds.isEmpty()) {
            menu.add(USE);
        }
        menu.add(DONE);
        return menu;
    }

    /** Ask the {@code from:} wiring of a streaming step; defaults to the previous step (or all tables). */
    private FromClause askFlow(String prevId) {
        String suggested = prevId == null ? "/.*/" : prevId;
        String answer = prompter.ask("From (upstream tables or step)", suggested);
        List<FromRef> refs = parseRefs(answer == null || answer.isBlank() ? suggested : answer);
        if (refs.isEmpty()) {
            // a token-less answer (e.g. just commas) falls back to the suggested default
            refs = parseRefs(suggested);
        }
        return FromClause.list(refs.toArray(FromRef[]::new));
    }

    private static List<FromRef> parseRefs(String spec) {
        List<FromRef> refs = new ArrayList<>();
        for (String token : spec.split(",")) {
            String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                refs.add(toRef(trimmed));
            }
        }
        return refs;
    }

    private static FromRef toRef(String token) {
        if (token.length() >= 2 && token.startsWith("/") && token.endsWith("/")) {
            return FromRef.regex(token.substring(1, token.length() - 1));
        }
        return FromRef.literal(token);
    }

    private String askId(String question, String suggested) {
        return askId(question, suggested, Set.of());
    }

    /**
     * Ask for an id, re-prompting until it is legal: no dot (the parser forbids it) and not one of the
     * {@code reserved} pipeline-internal ids. {@code suggested} must itself satisfy both, so a blank
     * (or exhausted) answer always terminates the loop on the default.
     */
    private String askId(String question, String suggested, Set<String> reserved) {
        while (true) {
            String answer = prompter.ask(question, suggested);
            String id = answer == null || answer.isBlank() ? suggested : answer;
            if (!id.contains(".") && !reserved.contains(id)) {
                return id;
            }
        }
    }

    /** Ask for a source id against the workspace's existing sources (shared with the standalone wizards). */
    private String askSourceRef(String question, boolean required) {
        return WizardPrompts.askSourceRef(prompter, existingSourceIds, question, required);
    }
}
