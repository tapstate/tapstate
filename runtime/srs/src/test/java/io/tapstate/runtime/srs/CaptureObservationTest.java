package io.tapstate.runtime.srs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStartedListener;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CaptureObservationTest {
    private static final CaptureObservation.Context CONTEXT = new CaptureObservation.Context(
            "capture", "opening", "chain", "source", "pipeline", "consumer", 7, 3, null, List.of("orders"));
    private static final CaptureListener ORIGINAL = (events, position) -> {};

    @Test
    void absentObservationPreservesTheExactPortListenerAndSubscription() {
        var expected = new Source();
        for (int i = 0; i < 2; i++) {
            assertThat(CaptureObservation.none().start(ORIGINAL, listener -> {
                assertThat(listener).isSameAs(ORIGINAL); return expected;
            })).isSameAs(expected);
        }
        assertThat(CaptureObservation.create(CaptureObservation.NONE, CONTEXT)).isSameAs(CaptureObservation.none());
    }

    @Test
    void onePhysicalOpenForwardsAcknowledgementsAndRetiresBeforeCloseExactlyOnce() {
        var order = new ArrayList<String>(); var factory = new Factory();
        factory.hooks.onRetire = () -> order.add("retire");
        var source = new Source(); source.onClose = () -> order.add("close");
        var observation = CaptureObservation.create(factory, CONTEXT);
        var opened = observation.start(ORIGINAL, listener -> { assertThat(listener).isSameAs(ORIGINAL); return source; });
        var position = new SourcePosition("opaque");
        opened.acknowledge(position); assertThat(source.ack).isSameAs(position);
        opened.close(); opened.close();
        opened.acknowledge(new SourcePosition("late")); assertThat(source.ack).isSameAs(position);
        assertThat(order).containsExactly("retire", "close");
        assertThat(factory.hooks.retired).hasValue(1); assertThat(source.closed).hasValue(1);
        assertThatThrownBy(() -> observation.start(ORIGINAL, listener -> source)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void factoryFailureIsReportedWithoutChangingTheSourceOperation() {
        var factory = new Factory(); factory.openFailure = new IllegalStateException("optional factory failed");
        var observation = CaptureObservation.create(factory, CONTEXT);
        var source = new Source();
        assertThat(observation.start(ORIGINAL, listener -> { assertThat(listener).isSameAs(ORIGINAL); return source; })).isSameAs(source);
        assertThat(factory.phases).containsExactly("open");
        assertThat(factory.failures).containsExactly(factory.openFailure);
    }

    @Test
    void wrappingFailureRetiresTheObserverAndUsesTheOriginalListener() {
        var factory = new Factory(); factory.hooks.wrapFailure = new IllegalStateException("optional wrapper failed");
        var source = new Source();
        var opened = CaptureObservation.create(factory, CONTEXT).start(ORIGINAL,
                listener -> { assertThat(listener).isSameAs(ORIGINAL); return source; });
        assertThat(factory.hooks.retired).hasValue(1); assertThat(factory.phases).containsExactly("wrap");
        assertThat(factory.hooks.reason).isSameAs(factory.hooks.wrapFailure);
        opened.close(); assertThat(factory.hooks.retired).hasValue(1); assertThat(source.closed).hasValue(1);
    }

    @Test
    void optionalStartingPositionCapabilityCannotBeRemovedOrAdded() {
        var anchors = new AtomicInteger();
        CaptureStartedListener started = new CaptureStartedListener() {
            @Override public void onStart(SourcePosition position) { anchors.incrementAndGet(); }
            @Override public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {}
        };
        var removes = new Factory(); removes.hooks.replacement = ORIGINAL;
        try (var source = CaptureObservation.create(removes, CONTEXT).start(started, listener -> {
            assertThat(listener).isSameAs(started); ((CaptureStartedListener) listener).onStart(new SourcePosition("start")); return new Source();
        })) { assertThat(anchors).hasValue(1); }
        assertThat(removes.hooks.retired).hasValue(1); assertThat(removes.phases).containsExactly("wrap");
        var adds = new Factory(); adds.hooks.replacement = started;
        try (var source = CaptureObservation.create(adds, CONTEXT).start(ORIGINAL, listener -> {
            assertThat(listener).isSameAs(ORIGINAL); assertThat(listener).isNotInstanceOf(CaptureStartedListener.class); return new Source();
        })) { assertThat(anchors).hasValue(1); }
        assertThat(adds.hooks.retired).hasValue(1);
    }

    @Test
    void sourceStartFailureKeepsItsIdentityAndRetiresTheObserver() {
        var original = new IllegalStateException("source refused start"); var factory = new Factory();
        assertThatThrownBy(() -> CaptureObservation.create(factory, CONTEXT).start(ORIGINAL, listener -> { throw original; })).isSameAs(original);
        assertThat(factory.hooks.retired).hasValue(1); assertThat(factory.hooks.reason).isSameAs(original);
        assertThat(factory.phases).isEmpty();
    }

    @Test
    void ordinaryRetirementFailureCannotSkipOrReplaceSourceClose() {
        var factory = new Factory(); factory.hooks.retireFailure = new IllegalStateException("observer retirement failed");
        var source = new Source(); var original = new IllegalStateException("source close failed"); source.closeFailure = original;
        var opened = CaptureObservation.create(factory, CONTEXT).start(ORIGINAL, listener -> source);
        assertThatThrownBy(opened::close).isSameAs(original);
        assertThat(source.closed).hasValue(1); assertThat(factory.phases).containsExactly("retire");
    }

    @Test
    void fatalRetirementStillClosesSourceAndDominatesItsOrdinaryFailure() {
        var factory = new Factory(); var fatal = new AssertionError("fatal observer"); factory.hooks.retireFailure = fatal;
        var source = new Source(); var ordinary = new IllegalStateException("source close"); source.closeFailure = ordinary;
        var opened = CaptureObservation.create(factory, CONTEXT).start(ORIGINAL, listener -> source);
        assertThatThrownBy(opened::close).isSameAs(fatal);
        assertThat(source.closed).hasValue(1); assertThat(fatal.getSuppressed()).contains(ordinary);
    }

    @Test
    void fatalSourceStartIsNotReplacedByLaterFatalRetirement() {
        var sourceFatal = new AssertionError("fatal source start"); var cleanupFatal = new AssertionError("fatal observer cleanup");
        var factory = new Factory(); factory.hooks.retireFailure = cleanupFatal;
        assertThatThrownBy(() -> CaptureObservation.create(factory, CONTEXT).start(ORIGINAL, listener -> { throw sourceFatal; })).isSameAs(sourceFatal);
        assertThat(sourceFatal.getSuppressed()).contains(cleanupFatal); assertThat(factory.hooks.retired).hasValue(1);
    }

    @Test
    void fatalReporterCannotSkipRetirementOfAnAlreadyCreatedObserver() {
        var factory = new Factory(); factory.hooks.wrapFailure = new IllegalStateException("wrap failed");
        var reporterFatal = new AssertionError("fatal diagnostic reporter"); factory.reportFailure = reporterFatal;
        var calls = new AtomicInteger();
        assertThatThrownBy(() -> CaptureObservation.create(factory, CONTEXT).start(ORIGINAL, listener -> {
            calls.incrementAndGet(); return new Source();
        })).isSameAs(reporterFatal);
        assertThat(factory.hooks.retired).hasValue(1); assertThat(calls).hasValue(0);
    }

    private static final class Factory implements CaptureObservation.Factory {
        private final Hooks hooks = new Hooks();
        private final List<String> phases = new ArrayList<>();
        private final List<RuntimeException> failures = new ArrayList<>();
        private RuntimeException openFailure;
        private Error reportFailure;
        @Override public CaptureObservation.Hooks open(CaptureObservation.Context context) {
            assertThat(context).isSameAs(CONTEXT); if (openFailure != null) throw openFailure; return hooks;
        }
        @Override public void failed(CaptureObservation.Context context, String phase, RuntimeException failure) {
            phases.add(phase); failures.add(failure); if (reportFailure != null) throw reportFailure;
        }
    }
    private static final class Hooks implements CaptureObservation.Hooks {
        private final AtomicInteger retired = new AtomicInteger();
        private Throwable wrapFailure;
        private Throwable retireFailure;
        private Throwable reason;
        private CaptureListener replacement;
        private Runnable onRetire = () -> {};
        @Override public CaptureListener wrap(CaptureListener original) { fail(wrapFailure); return replacement == null ? original : replacement; }
        @Override public void retire(Throwable reason) { this.reason = reason; retired.incrementAndGet(); onRetire.run(); fail(retireFailure); }
    }
    private static final class Source implements Subscription {
        private final AtomicInteger closed = new AtomicInteger();
        private SourcePosition ack;
        private Throwable closeFailure;
        private Runnable onClose = () -> {};
        @Override public void acknowledge(SourcePosition position) { ack = position; }
        @Override public void close() { closed.incrementAndGet(); onClose.run(); fail(closeFailure); }
    }
    private static void fail(Throwable failure) {
        if (failure instanceof RuntimeException ordinary) throw ordinary;
        if (failure instanceof Error fatal) throw fatal;
    }
}
