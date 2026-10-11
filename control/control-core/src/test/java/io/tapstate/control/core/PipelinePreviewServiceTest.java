package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.WriteMode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelinePreviewServiceTest {

    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-10-03T00:00:00Z"), ZoneOffset.UTC);

    @Test
    void acceptsCandidateDraftsAtTheConfiguredLimits() {
        List<ArtifactDraft> drafts = new ArrayList<>();
        for (int index = 0; index < PipelinePreviewService.MAX_DRAFTS; index++) {
            drafts.add(new ArtifactDraft("resource-" + index, "x"));
        }

        assertThat(PipelinePreviewService.requireDrafts(drafts)).hasSize(PipelinePreviewService.MAX_DRAFTS);
        assertThat(PipelinePreviewService.requireDrafts(List.of(new ArtifactDraft(
                "large", "x".repeat((int) PipelinePreviewService.MAX_DRAFT_BYTES)))))
                .hasSize(1);
    }

    @Test
    void refusesTooManyCandidateDraftsWithACodedInputError() {
        List<ArtifactDraft> drafts = new ArrayList<>();
        for (int index = 0; index <= PipelinePreviewService.MAX_DRAFTS; index++) {
            drafts.add(new ArtifactDraft("resource-" + index, "x"));
        }

        assertThatThrownBy(() -> PipelinePreviewService.requireDrafts(drafts))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code().code())
                .isEqualTo("control.malformed-request");
    }

    @Test
    void refusesCandidateContentAboveTheByteLimitWithACodedInputError() {
        ArtifactDraft oversized = new ArtifactDraft(
                "large", "x".repeat((int) PipelinePreviewService.MAX_DRAFT_BYTES + 1));

        assertThatThrownBy(() -> PipelinePreviewService.requireDrafts(List.of(oversized)))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code().code())
                .isEqualTo("control.malformed-request");
    }

    @Test
    void compilesTheCandidateClosureBuildsStepAndRelationContextsAndSequencesEvents() throws Exception {
        Fixture fixture = fixture();
        AtomicInteger cancellations = new AtomicInteger();
        AtomicInteger previews = new AtomicInteger();
        PipelinePreviewService service = new PipelinePreviewService(candidateCompiler(fixture.resources()), request -> {
            previews.incrementAndGet();
            assertThat(request.principal()).isEqualTo("author");
            assertThat(request.pipelineId()).isEqualTo("pipeline");
            assertThat(request.outputId()).isEqualTo("sync_out");
            assertThat(request.rootLimit()).isEqualTo(7);
            assertThat(request.sampleId()).isEqualTo("sample-a");
            assertThat(request.canonicalResources()).hasSize(4);
            return stream(cancellations);
        }, FIXED_CLOCK);

        PipelinePreviewSession session = service.open("author", new PipelinePreviewCommand(
                "pipeline", "sync_out", 7, "sample-a", List.of(new ArtifactDraft("candidate", "content"))));

        assertThat(session.sampleCacheHit()).isTrue();
        List<PipelinePreviewEvent> events = new ArrayList<>();
        PipelinePreviewEvent next;
        while ((next = session.next()) != null) {
            events.add(next);
        }
        assertThat(events).extracting(PipelinePreviewEvent::kind)
                .containsExactly("run.accepted", "compile.completed", "sample.completed", "run.completed");
        assertThat(events).extracting(PipelinePreviewEvent::seq).containsExactly(0L, 1L, 2L, 3L);
        assertThat(events).extracting(PipelinePreviewEvent::runId).doesNotContainNull();
        assertThat(events.get(0).payload()).containsEntry("consistency", "best-effort");
        PipelinePreviewExecutionSpec spec =
                (PipelinePreviewExecutionSpec) events.get(1).payload().get("executionSpec");
        assertThat(spec.outputKind()).isEqualTo("sync");
        assertThat(spec.outputId()).isEqualTo("sync_out");
        List<?> contexts = (List<?>) events.get(1).payload().get("contexts");
        assertThat(contexts).hasSize(10);
        assertThat(contexts).anySatisfy(value -> {
            Map<?, ?> context = (Map<?, ?>) value;
            if ("transform:map".equals(context.get("id"))) {
                assertThat((List<?>) context.get("effects")).hasSize(4);
                assertThat(((List<?>) context.get("pointerPatterns")).stream().map(String.class::cast).toList())
                        .contains("/renamed~1field", "/total", "/constant", "/secret");
            }
        }).anySatisfy(value -> {
            Map<?, ?> context = (Map<?, ?>) value;
            assertThat(context.get("id")).isEqualTo("relation:nest:items/labels");
            assertThat(context.get("parentPointerPattern")).isEqualTo("/items");
            assertThat(context.get("pointerPatterns")).isEqualTo(List.of("/items/labels"));
        });
        session.cancel();
        assertThat(cancellations).hasValue(1);
        assertThat(previews).hasValue(1);
        assertThat(session.next()).isNull();
    }

    @Test
    void defaultsRootLimitRejectsAmbiguousOutputsAndClosesItsCompilationPermit() throws Exception {
        Fixture fixture = fixture();
        PipelinePreviewService service = new PipelinePreviewService(candidateCompiler(fixture.resources()),
                request -> stream(new AtomicInteger()), FIXED_CLOCK);

        assertThatThrownBy(() -> service.open("author", new PipelinePreviewCommand(
                "pipeline", null, null, null, List.of(new ArtifactDraft("candidate", "content")))))
                .isInstanceOf(TapstateException.class);
        PipelinePreviewSession session = service.open("author", new PipelinePreviewCommand(
                "pipeline", "view_out", null, null,
                List.of(new ArtifactDraft("candidate", "content"))));
        assertThat(session.next().payload()).containsEntry("rootLimit", PipelinePreviewService.DEFAULT_ROOT_LIMIT);
        assertThatThrownBy(() -> service.open(" ", new PipelinePreviewCommand(
                "pipeline", "view_out", 1, null, List.of(new ArtifactDraft("candidate", "content")))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void validatesCommandBoundsAndHandlesTerminalOrPrematureExecutionEvents() throws Exception {
        Fixture fixture = fixture();
        PipelinePreviewService service = new PipelinePreviewService(candidateCompiler(fixture.resources()),
                request -> new io.tapstate.runtime.probe.PipelinePreviewStream() {
                    private boolean sent;

                    @Override
                    public boolean sampleCacheHit() {
                        return false;
                    }

                    @Override
                    public io.tapstate.runtime.probe.PipelinePreviewEvent next() {
                        if (sent) {
                            return null;
                        }
                        sent = true;
                        return new io.tapstate.runtime.probe.PipelinePreviewEvent(request.runId(),
                                request.candidateHash(), 0, "run.failed", FIXED_CLOCK.instant(), Map.of());
                    }

                    @Override
                    public void cancel() {
                    }
                }, FIXED_CLOCK);

        assertThatThrownBy(() -> service.open("author", new PipelinePreviewCommand(
                "pipeline", "view_out", 0, null, List.of(new ArtifactDraft("candidate", "content")))))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> service.open("author", new PipelinePreviewCommand(
                "pipeline", "view_out", 1, "x".repeat(129), List.of(new ArtifactDraft("candidate", "content")))))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewService.requireDrafts(null)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewService.requireDrafts(List.of())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewService.requireDrafts(java.util.Arrays.asList((ArtifactDraft) null)))
                .isInstanceOf(TapstateException.class);

        PipelinePreviewSession session = service.open("author", new PipelinePreviewCommand(
                "pipeline", "view_out", 1, null, List.of(new ArtifactDraft("candidate", "content"))));
        assertThat(session.next().kind()).isEqualTo("run.accepted");
        assertThat(session.next().kind()).isEqualTo("compile.completed");
        assertThat(session.next().kind()).isEqualTo("run.failed");
        assertThat(session.next()).isNull();
    }

    @Test
    void rejectsAnExecutionStreamThatEndsWithoutItsTerminalEvent() throws Exception {
        Fixture fixture = fixture();
        PipelinePreviewService service = new PipelinePreviewService(candidateCompiler(fixture.resources()),
                request -> new io.tapstate.runtime.probe.PipelinePreviewStream() {
                    @Override
                    public boolean sampleCacheHit() {
                        return false;
                    }

                    @Override
                    public io.tapstate.runtime.probe.PipelinePreviewEvent next() {
                        return null;
                    }

                    @Override
                    public void cancel() {
                    }
                }, FIXED_CLOCK);
        PipelinePreviewSession session = service.open("author", new PipelinePreviewCommand(
                "pipeline", "view_out", 1, null, List.of(new ArtifactDraft("candidate", "content"))));
        session.next();
        session.next();
        assertThatThrownBy(session::next).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("without a terminal event");
    }

    private static Fixture fixture() {
        SourceResource source = new SourceResource("source", null, "test", Map.of(), SourceMode.CDC,
                null, null, null);
        SourceResource target = new SourceResource("target", null, "test", Map.of(), SourceMode.CDC,
                null, null, null);
        Embed labels = new Embed("labels", Map.of("id", "id"), EmbedAs.ARRAY, "labels",
                null, null, null, null, null);
        Embed items = new Embed("items", Map.of("order_id", "id"), EmbedAs.ARRAY, "items",
                null, null, null, null, List.of(labels));
        Map<String, FieldRule> fields = new LinkedHashMap<>();
        fields.put("renamed/field", FieldRule.rename("old_name"));
        fields.put("total", FieldRule.computed("sum(after.items.amount)"));
        fields.put("constant", FieldRule.literal("value"));
        fields.put("secret", FieldRule.drop());
        Step map = Step.inline("map", FromClause.list(FromRef.literal("source.orders")),
                new TransformBody.MapProjection(fields), null);
        Step filter = Step.inline("filter", FromClause.list(FromRef.literal("map")),
                new TransformBody.Filter("true"), null);
        Step unwind = Step.inline("unwind", FromClause.list(FromRef.literal("filter")),
                new TransformBody.Unwind("items", "item_index", false, null, null), null);
        Step js = Step.inline("js", FromClause.list(FromRef.literal("unwind")),
                new TransformBody.Js("function process(record) { return record; }"), null);
        Step union = Step.inline("union", FromClause.list(FromRef.literal("js"), FromRef.literal("source.orders")),
                new TransformBody.Union(), null);
        Map<String, FromRef> aliases = Map.of(
                "root", FromRef.literal("source.orders"),
                "items", FromRef.literal("source.items"),
                "labels", FromRef.literal("source.labels"));
        Step nest = Step.inline("nest", FromClause.aliases(aliases),
                new TransformBody.Nest(null, null, new NestRoot("root", List.of("id"), null, null,
                        List.of(items))), null);
        Step reused = Step.use("reused", "shared", FromClause.list(FromRef.literal("nest")));
        TransformResource shared = new TransformResource("shared", null,
                new TransformBody.Filter("after.active == true"), null);
        ServeBlock.Inline serve = new ServeBlock.Inline("serve", FromRef.literal("reused"),
                List.of(new SyncElement("sync_out", "target", WriteMode.UPSERT, null, null)), null, null);
        PipelineResource pipeline = new PipelineResource("pipeline", null,
                List.of(SourceRef.spec("source", true)), List.of(map, filter, unwind, js, union, nest, reused),
                new ViewBlock.Inline("view_out", FromRef.literal("reused"), "id", null), serve, null, null);
        return new Fixture(pipeline, List.of(source, target, shared, pipeline));
    }

    private static PipelinePreviewService.CandidateWorkspaceCompiler candidateCompiler(List<Resource> resources) {
        return drafts -> new CandidateWorkspacePlan(new ApplyPlan(List.of()), resources);
    }

    private static io.tapstate.runtime.probe.PipelinePreviewStream stream(AtomicInteger cancellations) {
        return new io.tapstate.runtime.probe.PipelinePreviewStream() {
            private int next;

            @Override
            public boolean sampleCacheHit() {
                return true;
            }

            @Override
            public io.tapstate.runtime.probe.PipelinePreviewEvent next() {
                if (next++ == 0) {
                    return new io.tapstate.runtime.probe.PipelinePreviewEvent("runtime-run", "hash", 0,
                            "sample.completed", FIXED_CLOCK.instant(), Map.of("rows", 1));
                }
                return new io.tapstate.runtime.probe.PipelinePreviewEvent("runtime-run", "hash", 1,
                        "run.completed", FIXED_CLOCK.instant(), Map.of());
            }

            @Override
            public void cancel() {
                cancellations.incrementAndGet();
            }
        };
    }

    private record Fixture(PipelineResource pipeline, List<Resource> resources) {
    }

    private static final class EmptyArtifactStore implements io.tapstate.spi.store.ArtifactStore {
        @Override
        public void saveAll(List<Resource> artifacts) {
        }

        @Override
        public Optional<Resource> get(String id) {
            return Optional.empty();
        }

        @Override
        public List<Resource> list() {
            return List.of();
        }
    }
}
