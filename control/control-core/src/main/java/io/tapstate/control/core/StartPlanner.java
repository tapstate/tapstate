package io.tapstate.control.core;

import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.StartLoad;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Predicts, before a start writes anything, how that start will load each target its pipeline writes.
 *
 * <p>The judgement is {@link StartLoad}'s, the one the run's preparation also makes; what this adds is
 * the two things only the verb knows, because it is predicting what is about to be true rather than
 * reading what is true now:
 *
 * <ul>
 *   <li>a start written over a stop that asked to clear is carried out as "stop, clearing, then start",
 *       whether or not that stop has finished yet -- so the pipeline's records count as gone;</li>
 *   <li>a rerun is a clearing stop followed by a start, so the same holds for it, on a chain other
 *       pipelines still read as much as on one this pipeline has alone: what this pipeline loaded is
 *       recorded on its own record, which the clearing stop takes away.</li>
 * </ul>
 */
public final class StartPlanner {

    private final java.util.function.BiFunction<PipelineResource, Optional<io.tapstate.spi.store.PipelineIntentScope>, Reads> capture;

    public record Reads(Supplier<List<String>> progressChains,
            Function<String, Optional<SrsMeta>> progress,
            Supplier<List<PipelineWriteTargets.WriteTarget>> targets, Runnable verify,
            Supplier<Optional<DesiredStore.StartPreparation>> preparation,
            Function<String, Optional<io.tapstate.core.model.SourceResource>> targetConnections) {
        public Reads(Supplier<List<String>> progressChains, Function<String, Optional<SrsMeta>> progress,
                Supplier<List<PipelineWriteTargets.WriteTarget>> targets, Runnable verify,
                Supplier<Optional<DesiredStore.StartPreparation>> preparation) {
            this(progressChains, progress, targets, verify, preparation, ignored -> Optional.empty());
        }
    }

    public StartPlanner(PipelineChains chains, SrsMetaStore meta, PipelineWriteTargets targets) {
        this(definition -> new Reads(() -> chains.progressChainsOf(definition.id()), meta::read,
                () -> targets.of(definition), chains.readingGuard(definition.id()), Optional::empty));
        Objects.requireNonNull(chains, "chains");
        Objects.requireNonNull(targets, "targets");
        Objects.requireNonNull(meta, "meta");
    }

    public StartPlanner(Function<PipelineResource, Reads> capture) {
        this((definition, scope) -> capture.apply(definition));
        Objects.requireNonNull(capture, "capture");
    }

    public StartPlanner(java.util.function.BiFunction<PipelineResource, Optional<io.tapstate.spi.store.PipelineIntentScope>, Reads> capture) {
        this.capture = Objects.requireNonNull(capture, "capture");
    }

    /**
     * The plan for starting {@code definition} now.
     *
     * @param definition the definition the start would run; under evaluation of a change a person chose,
     *                   the changed one, which writes the same targets with different policies
     * @param prior      the pipeline's last desired intent, empty for a pipeline never started
     * @param intent     which start is being predicted
     */
    public StartPlan plan(PipelineResource definition, Optional<DesiredState> prior, StartIntent intent) {
        return plan(definition, prior, intent, Optional.empty());
    }

    StartPlan plan(PipelineResource definition, Optional<DesiredState> prior, StartIntent intent,
            Optional<DesiredStore.StartIntent> original) {
        Objects.requireNonNull(definition, "definition");
        Objects.requireNonNull(prior, "prior");
        Objects.requireNonNull(intent, "intent");
        var scope = original.flatMap(DesiredStore.StartIntent::scope);
        Reads reads = capture.apply(definition, scope);
        boolean firstStart = scope.isPresent() && original.orElseThrow().revision() == 0;
        ReadMode mode = readMode(definition);
        StartLoad load = StartLoad.of(mode, definition.id(),
                mode == ReadMode.CDC_ONLY || firstStart ? List.of() : recordsAtStart(reads, prior, intent));
        var result=new StartPlan(load, reads.targets().get().stream()
                .map(target -> new StartPlan.Entry(target, load))
                .toList(), clearsFirst(prior, intent) ? Optional::empty : reads.preparation(), reads.targetConnections());
        reads.verify().run();
        return result;
    }

    /** What this pipeline will have recorded on its chains when the predicted start reaches them. */
    private List<SrsMeta> recordsAtStart(
            Reads reads, Optional<DesiredState> prior, StartIntent intent) {
        if (clearsFirst(prior, intent)) {
            return List.of();
        }
        return reads.progressChains().get().stream()
                .map(reads.progress())
                .flatMap(Optional::stream)
                .toList();
    }

    /**
     * Whether the start being predicted runs only after this pipeline's records have been cleared: a
     * rerun always does, and so does a start over a stop that asked to clear.
     */
    static boolean clearsFirst(Optional<DesiredState> prior, StartIntent intent) {
        if (intent == StartIntent.RERUN) {
            return true;
        }
        return prior.map(desired -> desired.targetState() == PipelineState.STOPPED && desired.purgeState())
                .orElse(false);
    }

    private static ReadMode readMode(PipelineResource definition) {
        return definition.settings() == null ? null : definition.settings().readMode();
    }
}
