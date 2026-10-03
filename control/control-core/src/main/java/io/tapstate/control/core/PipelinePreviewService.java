package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.ViewResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.runtime.probe.PipelinePreviewProbe;
import io.tapstate.runtime.probe.PipelinePreviewRequest;
import io.tapstate.runtime.probe.PipelinePreviewStream;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;

/** Compiles unsaved candidates before opening a bounded, read-only Pipeline preview session. */
public final class PipelinePreviewService {

    public static final int DEFAULT_ROOT_LIMIT = 100;
    public static final Duration DEADLINE = Duration.ofSeconds(15);
    public static final int MAX_DRAFTS = 256;
    public static final long MAX_DRAFT_BYTES = 4L * 1024L * 1024L;
    private static final int MAX_CONCURRENT_COMPILATIONS = 8;

    private final CandidateWorkspaceCompiler compiler;
    private final PipelinePreviewProbe probe;
    private final CanonicalWriter writer = new CanonicalWriter();
    private final Clock clock;
    private final Semaphore compileSlots = new Semaphore(MAX_CONCURRENT_COMPILATIONS);

    public PipelinePreviewService(ApplyService compiler, PipelinePreviewProbe probe, Clock clock) {
        this(Objects.requireNonNull(compiler, "compiler")::planCandidateWorkspace, probe, clock);
    }

    PipelinePreviewService(CandidateWorkspaceCompiler compiler, PipelinePreviewProbe probe, Clock clock) {
        this.compiler = Objects.requireNonNull(compiler, "compiler");
        this.probe = Objects.requireNonNull(probe, "probe");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Compiles the candidate closure and opens a single-use event stream. */
    public PipelinePreviewSession open(String principal, PipelinePreviewCommand command) {
        if (principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("principal must be non-blank");
        }
        Objects.requireNonNull(command, "command");
        String pipelineId = requireText(command.pipelineId(), "pipelineId");
        List<ArtifactDraft> drafts = requireDrafts(command.drafts());
        int rootLimit = command.rootLimit() == null ? DEFAULT_ROOT_LIMIT : command.rootLimit();
        if (rootLimit < 1 || rootLimit > PipelinePreviewRequest.MAX_ROOT_LIMIT) {
            throw malformed("rootLimit must be between 1 and " + PipelinePreviewRequest.MAX_ROOT_LIMIT);
        }
        String sampleId = command.sampleId();
        if (sampleId != null && sampleId.length() > PipelinePreviewRequest.MAX_SAMPLE_ID_LENGTH) {
            throw malformed("sampleId exceeds the 128 character limit");
        }

        Instant deadline = clock.instant().plus(DEADLINE);
        if (!compileSlots.tryAcquire()) {
            throw new TapstateException(ControlError.PREVIEW_OVERLOADED, Map.of(), null);
        }
        PipelinePreviewRequest request;
        PipelinePreviewExecutionSpec spec;
        String candidateHash;
        String runId;
        try {
            CandidateWorkspacePlan candidate = compiler.compile(drafts);
            Map<String, Resource> resources = new LinkedHashMap<>();
            candidate.resources().forEach(resource -> resources.put(resource.id(), resource));
            Resource requested = resources.get(pipelineId);
            if (!(requested instanceof PipelineResource pipeline)) {
                throw malformed("pipelineId must identify a Pipeline in the candidate workspace");
            }

            Output output = selectOutput(pipeline, resources, command.outputId());
            List<Resource> closure = executionClosure(pipeline, output, resources);
            candidateHash = candidateHash(closure);
            runId = UUID.randomUUID().toString();
            List<String> canonical = closure.stream()
                    .sorted(Comparator.comparing(Resource::id))
                    .map(writer::write)
                    .toList();
            request = new PipelinePreviewRequest(
                    runId, principal, pipelineId, output.id(), rootLimit, sampleId, candidateHash,
                    canonical, deadline);
            spec = new PipelinePreviewExecutionSpec(
                    pipelineId, candidateHash, publicPipeline(pipeline, output), output.id(), output.kind(),
                    rootLimit, "best-effort", contexts(pipeline, resources));
        } finally {
            compileSlots.release();
        }
        PipelinePreviewStream execution = probe.preview(request);
        return new SequencedStream(runId, candidateHash, spec, pipelineId, rootLimit, execution, clock);
    }

    static List<ArtifactDraft> requireDrafts(List<ArtifactDraft> drafts) {
        if (drafts == null || drafts.isEmpty()) {
            throw malformed("drafts must contain the candidate workspace resources");
        }
        if (drafts.size() > MAX_DRAFTS) {
            throw malformed("drafts exceed the " + MAX_DRAFTS + " resource limit");
        }
        long contentBytes = 0;
        for (ArtifactDraft draft : drafts) {
            if (draft == null || draft.content() == null || draft.content().isBlank()) {
                throw malformed("each draft must carry non-blank content");
            }
            contentBytes += draft.content().getBytes(StandardCharsets.UTF_8).length;
            if (contentBytes > MAX_DRAFT_BYTES) {
                throw malformed("draft content exceeds the 4 MiB limit");
            }
        }
        return List.copyOf(drafts);
    }

    private static Output selectOutput(
            PipelineResource pipeline, Map<String, Resource> resources, String selectedId) {
        List<Output> outputs = new ArrayList<>();
        if (pipeline.view() instanceof ViewBlock.Inline view) {
            outputs.add(new Output(view.id(), "view", view));
        } else if (pipeline.view() instanceof ViewBlock.Use view) {
            requireKind(resources, view.use(), ViewResource.class, "view");
            outputs.add(new Output(view.id(), "view", view));
        }
        ServeBlock serve = pipeline.serve();
        if (serve instanceof ServeBlock.Use use) {
            ServeResource definition = requireKind(resources, use.use(), ServeResource.class, "serve");
            serve = new ServeBlock.Inline(use.id(), use.from(), definition.sync(), definition.query(), definition.push());
        }
        if (serve instanceof ServeBlock.Inline inline && inline.sync() != null) {
            for (int index = 0; index < inline.sync().size(); index++) {
                var sync = inline.sync().get(index);
                outputs.add(new Output(sync.id() == null ? "sync_" + index : sync.id(), "sync", sync));
            }
        }
        if (outputs.isEmpty()) {
            throw malformed("the Pipeline has no previewable view or sync output");
        }
        if (selectedId == null) {
            if (outputs.size() != 1) {
                throw malformed("outputId is required when the Pipeline has multiple outputs");
            }
            return outputs.getFirst();
        }
        List<Output> matching = outputs.stream().filter(output -> output.id().equals(selectedId)).toList();
        if (matching.size() != 1) {
            throw malformed("outputId must identify exactly one candidate Pipeline output");
        }
        return matching.getFirst();
    }

    private static <T extends Resource> T requireKind(
            Map<String, Resource> resources, String id, Class<T> kind, String label) {
        Resource resource = resources.get(id);
        if (!kind.isInstance(resource)) {
            throw malformed("candidate " + label + " resource '" + id + "' is missing");
        }
        return kind.cast(resource);
    }

    private static List<Resource> executionClosure(
            PipelineResource pipeline, Output output, Map<String, Resource> resources) {
        LinkedHashSet<String> ids = new LinkedHashSet<>();
        ids.add(pipeline.id());
        ids.addAll(pipeline.sourceIds());
        if (pipeline.transforms() != null) {
            for (Step step : pipeline.transforms()) {
                if (step instanceof Step.Use use) {
                    ids.add(use.use());
                }
            }
        }
        if (pipeline.view() instanceof ViewBlock.Use view) {
            ids.add(view.use());
        }
        if (pipeline.serve() instanceof ServeBlock.Use serve) {
            ids.add(serve.use());
        }
        if ("sync".equals(output.kind())) {
            var sync = (io.tapstate.core.model.SyncElement) output.definition();
            ids.add(sync.source());
        }
        List<Resource> closure = new ArrayList<>();
        for (String id : ids) {
            Resource resource = resources.get(id);
            if (resource == null) {
                throw malformed("candidate resource '" + id + "' is missing");
            }
            closure.add(resource);
        }
        return List.copyOf(closure);
    }

    private String candidateHash(List<Resource> resources) {
        List<String> material = resources.stream().sorted(Comparator.comparing(Resource::id))
                .map(resource -> resource.id() + ":" + CanonicalHash.of(resource)).toList();
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(String.join("\n", material).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static PipelineResource publicPipeline(PipelineResource pipeline, Output output) {
        return pipeline;
    }

    private static List<Map<String, Object>> contexts(
            PipelineResource pipeline, Map<String, Resource> resources) {
        List<Map<String, Object>> contexts = new ArrayList<>();
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("id", "root");
        root.put("kind", "root");
        root.put("sourceIds", pipeline.sourceIds());
        root.put("pointerPatterns", List.of());
        contexts.add(Map.copyOf(root));
        if (pipeline.transforms() != null) {
            for (Step step : pipeline.transforms()) {
                TransformBody body = body(step, resources);
                if (body instanceof TransformBody.Nest nest) {
                    for (Embed embed : nest.root().embed() == null ? List.<Embed>of() : nest.root().embed()) {
                        addRelationContexts(contexts, step.id(), embed, "");
                    }
                }
                contexts.add(transformContext(step, body));
            }
        }
        return List.copyOf(contexts);
    }

    private static void addRelationContexts(List<Map<String, Object>> into, String stepId, Embed embed, String parent) {
        String segment = embed.path() == null ? embed.treeSegment() : embed.path();
        String path = parent.isEmpty() ? segment : parent + "/" + segment;
        Map<String, Object> relation = new LinkedHashMap<>();
        relation.put("id", "relation:" + stepId + ":" + path);
        relation.put("kind", "relation");
        relation.put("stepId", stepId);
        relation.put("path", path);
        relation.put("parentPath", parent);
        relation.put("parentPointerPattern", parent.isEmpty() ? "" : pointerPath(parent));
        relation.put("shape", embed.as().yaml());
        relation.put("sourceAlias", embed.from());
        relation.put("joinKeys", embed.on());
        relation.put("pointerPatterns", embed.path() == null ? List.of() : List.of(pointerPath(path)));
        relation.put("pointerPatternsComplete", embed.path() != null);
        into.add(Map.copyOf(relation));
        if (embed.embed() != null) {
            for (Embed child : embed.embed()) {
                addRelationContexts(into, stepId, child, path);
            }
        }
    }

    private static Map<String, Object> transformContext(Step step, TransformBody body) {
        String type = body == null ? "unknown" : body.type();
        Map<String, Object> context = new LinkedHashMap<>();
        context.put("id", "transform:" + step.id());
        context.put("kind", "transform");
        context.put("stepId", step.id());
        context.put("type", type);
        List<Map<String, Object>> effects = new ArrayList<>();
        List<String> pointers = new ArrayList<>();
        boolean pointerPatternsComplete = true;
        if (body instanceof TransformBody.MapProjection map) {
            map.fields().forEach((output, rule) -> {
                String pointer = pointerField(output);
                Map<String, Object> effect = new LinkedHashMap<>();
                effect.put("pointer", pointer);
                if (rule instanceof FieldRule.Drop) {
                    effect.put("kind", "removed");
                    effect.put("annotation", "map: drop " + output);
                } else if (rule instanceof FieldRule.Rename rename) {
                    effect.put("kind", "renamed");
                    effect.put("origin", rename.sourceField());
                    effect.put("annotation", "map: rename " + rename.sourceField() + " -> " + output);
                } else if (rule instanceof FieldRule.Computed computed) {
                    effect.put("kind", "computed");
                    effect.put("origin", "computed");
                    effect.put("annotation", "map: " + abbreviate(computed.celExpr()));
                } else if (rule instanceof FieldRule.Literal) {
                    effect.put("kind", "literal");
                    effect.put("origin", "literal");
                    effect.put("annotation", "map: literal");
                }
                effects.add(Map.copyOf(effect));
                pointers.add(pointer);
            });
        } else if (body instanceof TransformBody.Unwind unwind) {
            pointers.add(pointerPath(unwind.path()));
            if (unwind.includeArrayIndex() != null && !unwind.includeArrayIndex().isBlank()) {
                pointers.add(pointerField(unwind.includeArrayIndex()));
            }
            context.put("countEffect", "fan-out");
        } else if (body instanceof TransformBody.Filter) {
            context.put("countEffect", "drop");
        } else if (body instanceof TransformBody.Js) {
            context.put("origin", "unknown");
            context.put("lastChangedBy", step.id());
            context.put("determinism", "unknown");
            pointerPatternsComplete = false;
        } else if (body instanceof TransformBody.Union) {
            context.put("countEffect", "merge");
        } else if (body instanceof TransformBody.Nest) {
            context.put("countEffect", "assemble");
        } else if (body instanceof TransformBody.Join) {
            context.put("countEffect", "join");
            pointerPatternsComplete = false;
        }
        context.put("pointerPatterns", List.copyOf(pointers));
        context.put("pointerPatternsComplete", pointerPatternsComplete);
        context.put("effects", List.copyOf(effects));
        return Map.copyOf(context);
    }

    private static TransformBody body(Step step, Map<String, Resource> resources) {
        if (step instanceof Step.Inline inline) {
            return inline.body();
        }
        if (step instanceof Step.Use use && resources.get(use.use()) instanceof TransformResource transform) {
            return transform.body();
        }
        return null;
    }

    private static String pointerPath(String path) {
        String[] segments = path.contains("/") ? path.split("/", -1) : path.split("\\.", -1);
        StringBuilder pointer = new StringBuilder();
        for (String segment : segments) {
            pointer.append('/').append(escapePointerSegment(segment));
        }
        return pointer.toString();
    }

    private static String pointerField(String field) {
        return "/" + escapePointerSegment(field);
    }

    private static String escapePointerSegment(String segment) {
        return segment.replace("~", "~0").replace("/", "~1");
    }

    private static String abbreviate(String expression) {
        return expression.length() <= 256 ? expression : expression.substring(0, 253) + "...";
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw malformed(name + " must be non-blank");
        }
        return value;
    }

    private static TapstateException malformed(String reason) {
        return new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", reason), null);
    }

    private record Output(String id, String kind, Object definition) {
    }

    @FunctionalInterface
    interface CandidateWorkspaceCompiler {
        CandidateWorkspacePlan compile(List<ArtifactDraft> drafts);
    }

    private static final class SequencedStream implements PipelinePreviewSession {

        private final String runId;
        private final String candidateHash;
        private final PipelinePreviewExecutionSpec executionSpec;
        private final String pipelineId;
        private final int rootLimit;
        private final PipelinePreviewStream delegate;
        private final Clock clock;
        private long sequence;
        private int initial;
        private boolean terminal;

        SequencedStream(String runId, String candidateHash, PipelinePreviewExecutionSpec executionSpec,
                String pipelineId, int rootLimit, PipelinePreviewStream delegate, Clock clock) {
            this.runId = runId;
            this.candidateHash = candidateHash;
            this.executionSpec = executionSpec;
            this.pipelineId = pipelineId;
            this.rootLimit = rootLimit;
            this.delegate = Objects.requireNonNull(delegate, "delegate");
            this.clock = clock;
        }

        @Override
        public boolean sampleCacheHit() {
            return delegate.sampleCacheHit();
        }

        @Override
        public synchronized PipelinePreviewEvent next() throws InterruptedException {
            if (terminal) {
                return null;
            }
            if (initial == 0) {
                initial++;
                return event("run.accepted", Map.of("pipelineId", pipelineId, "rootLimit", rootLimit,
                        "consistency", "best-effort"));
            }
            if (initial == 1) {
                initial++;
                return event("compile.completed", Map.of(
                        "executionSpec", executionSpec,
                        "contexts", executionSpec.contexts(),
                        "candidateHash", candidateHash));
            }
            io.tapstate.runtime.probe.PipelinePreviewEvent next = delegate.next();
            if (next == null) {
                throw new IllegalStateException("preview execution ended without a terminal event");
            }
            PipelinePreviewEvent sequenced = event(next.kind(), next.payload());
            if ("run.completed".equals(next.kind()) || "run.failed".equals(next.kind())) {
                terminal = true;
            }
            return sequenced;
        }

        private PipelinePreviewEvent event(String kind, Map<String, Object> payload) {
            return new PipelinePreviewEvent(runId, candidateHash, sequence++, kind, clock.instant(), payload);
        }

        @Override
        public void cancel() {
            delegate.cancel();
        }
    }
}
