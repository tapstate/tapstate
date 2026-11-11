package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Serial native counts of fixed table pairs on admitted blocking I/O, without row or root materialization. */
public final class ValidationCountScan {
    public record Scalar(ValidationTaskSpec.Table table, long count, ValidationCheckEvent.ReadBoundary read) {
        public Scalar { Objects.requireNonNull(table); Objects.requireNonNull(read); if (count < 0) throw invalid("negative"); }
    }
    @FunctionalInterface public interface Counter { Scalar read(Runnable requireCurrent); }
    public record Mapping(String id, Counter source, Counter target) {
        public Mapping { ValidationError.fieldName(id, "countScan.mapping"); Objects.requireNonNull(source); Objects.requireNonNull(target); }
    }
    private final ValidationRunIdentity identity;
    private final ValidationRunSpec spec;
    private final List<Mapping> mappings;
    private final Runnable requireCurrent;
    private final Consumer<ValidationCountCheck> publish;
    private final java.util.function.BiConsumer<String, ValidationPairException> publishFailure;
    private final AtomicBoolean attempted = new AtomicBoolean(), cancelled = new AtomicBoolean();
    private volatile boolean complete;
    private volatile int completedMappings;
    private volatile String readingMapping;

    public ValidationCountScan(ValidationRunIdentity identity, ValidationRunSpec spec, List<Mapping> mappings,
            Runnable requireCurrent, Consumer<ValidationCountCheck> publish) {
        this(identity, spec, mappings, requireCurrent, publish, (mapping, failed) -> { throw failed.diagnosis(); });
    }
    public ValidationCountScan(ValidationRunIdentity identity, ValidationRunSpec spec, List<Mapping> mappings,
            Runnable requireCurrent, Consumer<ValidationCountCheck> publish,
            java.util.function.BiConsumer<String, ValidationPairException> publishFailure) {
        this.publishFailure = Objects.requireNonNull(publishFailure);
        this.identity = Objects.requireNonNull(identity); this.spec = Objects.requireNonNull(spec);
        this.mappings = List.copyOf(mappings); this.requireCurrent = Objects.requireNonNull(requireCurrent); this.publish = Objects.requireNonNull(publish);
        if (identity.mode() != ValidationMode.FULL || spec.method() != ValidationMethod.COUNT
                || !identity.specHash().equals(spec.fingerprint()) || this.mappings.size() != spec.task().mappings().size()) throw invalid("scope");
        for (int index = 0; index < mappings.size(); index++)
            if (!mappings.get(index).id().equals(spec.task().mappings().get(index).mappingId())) throw invalid("mappings");
    }
    public void cancel() { cancelled.set(true); }
    public boolean complete() { return complete; }
    public int completedMappings(){return completedMappings;}
    public String readingMapping(){return readingMapping;}
    public void run() {
        if (!attempted.compareAndSet(false, true)) throw invalid("alreadyAttempted");
        for (int index = 0; index < mappings.size(); index++) {
            check(); var mapping = mappings.get(index); var declared = spec.task().mappings().get(index);
            readingMapping=mapping.id();
            ValidationCountCheck result = null;
            try {
                var source = Objects.requireNonNull(mapping.source().read(this::check)); check();
                if (!source.table().equals(declared.source())) throw invalid("sourceTable");
                var target = Objects.requireNonNull(mapping.target().read(this::check)); check();
                if (!target.table().equals(declared.target())) throw invalid("targetTable");
                result = new ValidationCountCheck(identity, mapping.id(), declared.outputId(), source.count(), target.count(), source.read(), target.read());
            } catch (ValidationPairException failure) {
                check();
                publishFailure.accept(mapping.id(), failure);
            }
            if (result != null) publish.accept(result);
            check();
            completedMappings=index+1;readingMapping=null;
        }
        check(); complete = true;
    }
    private void check() {
        if (cancelled.get()) throw cancelled();
        requireCurrent.run();
        if (cancelled.get()) throw cancelled();
    }
    private TapstateException cancelled() { return new TapstateException(ValidationError.EXECUTION_CANCELLED, Map.of("node", identity.owner().nodeId()), null); }
    private static TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "countScan." + field), null); }
}
