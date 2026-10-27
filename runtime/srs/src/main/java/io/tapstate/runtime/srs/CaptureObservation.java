package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStartedListener;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.WorkloadClaimFence;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Optional observer lifecycle for one physical tail opening, separate from normal source processing.
 * Context contains identity/routing metadata only, never connection settings, row images or positions.
 * Observation wraps the listener after capture health recording so it can see source failures as well.
 */
public final class CaptureObservation {
    public record Context(String captureId, String openingId, String chainId, String sourceId, String pipelineId, String consumerId,
                          long epoch, long schemaVersion, WorkloadClaimFence fence, List<String> tables) {
        public Context {
            Objects.requireNonNull(captureId, "captureId"); Objects.requireNonNull(openingId, "openingId");
            Objects.requireNonNull(chainId, "chainId");
            Objects.requireNonNull(sourceId, "sourceId"); Objects.requireNonNull(pipelineId, "pipelineId");
            Objects.requireNonNull(consumerId, "consumerId"); tables = List.copyOf(tables);
        }
    }
    public interface Hooks {
        CaptureListener wrap(CaptureListener original);
        /**
         * A bounded local retirement signal only. It may run under a physical-tail lifecycle lock.
         * Never wait for transport, perform control I/O, or complete observer drain/user callbacks here.
         * Null reason denotes ordinary close; actual cleanup/drain belongs to the observer's background owner.
         */
        void retire(Throwable reason);
    }
    @FunctionalInterface
    public interface Factory {
        /** Lifecycle path only. The factory owns cleanup of partial allocations if construction fails. */
        Hooks open(Context context);
        /** Records observer unavailability in its own subsystem; must be bounded and callback-free. */
        default void failed(Context context, String phase, RuntimeException failure) {}
    }
    private static final System.Logger LOG = System.getLogger(CaptureObservation.class.getName());
    private static final Hooks NO_HOOKS = new Hooks() {
        @Override public CaptureListener wrap(CaptureListener original) { return original; }
        @Override public void retire(Throwable reason) {}
    };
    public static final Factory NONE = context -> NO_HOOKS;
    private static final CaptureObservation UNOBSERVED = new CaptureObservation(null, NONE, NO_HOOKS);
    private final Context context;
    private final Factory factory;
    private final Hooks hooks;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean retired = new AtomicBoolean();

    private CaptureObservation(Context context, Factory factory, Hooks hooks) {
        this.context = context; this.factory = factory; this.hooks = hooks;
    }
    public static CaptureObservation none() { return UNOBSERVED; }

    public static CaptureObservation create(Factory factory, Context context) {
        Objects.requireNonNull(factory, "factory"); Objects.requireNonNull(context, "context");
        if (factory == NONE) return UNOBSERVED;
        try { return new CaptureObservation(context, factory, Objects.requireNonNull(factory.open(context), "observer hooks")); }
        catch (RuntimeException failure) {
            report(factory, context, "open", failure);
            return UNOBSERVED;
        }
    }

    /**
     * Opens exactly the requested source once. Optional observer failures fall back to the original
     * listener; source-start failures remain the source's own failures. No source capability is wrapped
     * or hidden: the caller still invokes its original port with the original config and start anchor.
     */
    public Subscription start(CaptureListener original, Function<CaptureListener, Subscription> source) {
        Objects.requireNonNull(original, "original"); Objects.requireNonNull(source, "source");
        if (this == UNOBSERVED) return source.apply(original);
        if (!started.compareAndSet(false, true)) throw new IllegalStateException("capture observation was opened twice");
        CaptureListener selected = original;
        try {
            if (!retired.get()) {
                selected = Objects.requireNonNull(hooks.wrap(original), "observed listener");
                if ((original instanceof CaptureStartedListener) != (selected instanceof CaptureStartedListener)) {
                    throw new IllegalStateException("capture observation changed the optional starting-position contract");
                }
            }
        } catch (RuntimeException failure) {
            retireAfterFailure(failure);
            report(factory, context, "wrap", failure);
            selected = original;
        } catch (Error fatal) {
            retireAfterFailure(fatal);
            throw fatal;
        }
        final Subscription underlying;
        try { underlying = Objects.requireNonNull(source.apply(selected), "source subscription"); }
        catch (RuntimeException | Error failure) {
            retireAfterFailure(failure);
            throw failure;
        }
        return new Subscription() {
            private final AtomicBoolean closing = new AtomicBoolean();
            private boolean ended;
            @Override public void acknowledge(SourcePosition position) { if (!closing.get()) underlying.acknowledge(position); }
            @Override public synchronized void close() {
                if (ended) return;
                closing.set(true);
                Error observerFatal = null;
                try { retire(null); }
                catch (Error fatal) { observerFatal = fatal; }
                try { underlying.close(); ended = true; }
                catch (RuntimeException | Error failure) {
                    if (observerFatal != null) { context(observerFatal, failure); throw observerFatal; }
                    throw failure;
                }
                if (observerFatal != null) throw observerFatal;
            }
        };
    }

    /** Idempotent, callback-free signal; it is not proof of observer worker/resource exit. */
    public void retire(Throwable reason) {
        if (this == UNOBSERVED || !retired.compareAndSet(false, true)) return;
        try { hooks.retire(reason); }
        catch (RuntimeException failure) { report(factory, context, "retire", failure); }
    }
    private void retireAfterFailure(Throwable failure) {
        try { retire(failure); }
        catch (Error cleanup) {
            if (failure instanceof Error fatal) context(fatal, cleanup);
            else { context(cleanup, failure); throw cleanup; }
        }
    }
    private static void report(Factory factory, Context context, String phase, RuntimeException failure) {
        var coded = new TapstateException(CaptureError.OBSERVER_FAILED,
                Map.of("captureId", context.captureId(), "openingId", context.openingId(), "phase", phase), failure);
        try { factory.failed(context, phase, failure); }
        catch (RuntimeException reporting) { context(coded, reporting); }
        catch (Error fatal) { context(fatal, coded); throw fatal; }
        LOG.log(System.Logger.Level.WARNING, coded.toString(), coded);
    }
    private static void context(Throwable primary, Throwable secondary) {
        if (primary != secondary && primary.getCause() != secondary && secondary.getCause() != primary) {
            primary.addSuppressed(secondary);
        }
    }
}
