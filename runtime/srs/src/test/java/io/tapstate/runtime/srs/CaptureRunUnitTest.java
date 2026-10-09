package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.logging.LogSink;
import io.tapstate.spi.store.IoError;
import io.tapstate.core.event.ChainPosition;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.ReadMode;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.SharedNotes;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.LogScopedCapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.ConnectionReport;
import io.tapstate.spi.capture.DiscoveredSchema;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.SnapshotSession;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.ConsumerProgressKind;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.SrsLogStore;
import io.tapstate.spi.store.SrsLogRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The capture run unit assembles the snapshot phase, cdc phase, the self-built Jet ring source and the
 * mining-chain coordinator into one source run, dispatched by the pipeline's consumption plan (its read
 * mode and its {@code srs.enabled} flag). It runs over a single embedded Hazelcast member sized to the L1
 * hot-buffer shape (capacity 8): a real per-table change ring for the shared-ring cdc paths, and a mock
 * connector (a fixed snapshot batch and a fixed change stream) standing in for a real PDK source.
 */
class CaptureRunUnitTest {

    @Test
    void theLastSharedSubscriberRetriesItsExactStreamUntilNativeCloseSucceeds() {
        HazelcastInstance member = emptyDurableMember();
        String key = "shared-strict-close-retry";
        ControlledSharedClosePort port = new ControlledSharedClosePort(key);
        CaptureRun first = null, last = null;
        try {
            InMemoryMeta meta = new InMemoryMeta();
            CaptureRunUnit unit = new CaptureRunUnit(port, new SrsCoordinator(meta), meta, member);
            first = unit.start(specFor("close-first", ReadMode.CDC_ONLY, key), row -> { });
            last = unit.start(specFor("close-last", ReadMode.CDC_ONLY, key), row -> { });
            assertThat(first.chainId()).isEqualTo(last.chainId());
            assertThat(port.opened).as("both actual runs retain one shared physical subscription").hasSize(1);
            Subscription physical = port.opened.getFirst();

            first.close();
            assertThat(port.attempted).as("the other subscriber still owns the shared tail").isEmpty();
            CaptureRun exactLast = last;
            assertThatThrownBy(exactLast::close).isSameAs(port.refused);
            assertThat(port.attempted).containsExactly(physical);
            assertThat(port.ended).as("a refused close is not a successful end").isFalse();

            assertThatThrownBy(exactLast::close).isSameAs(port.refused);
            assertThat(port.attempted).as("retry must reach that same still-unended stream, not return normally")
                    .containsExactly(physical, physical);
            assertThat(port.opened).hasSize(1);
            assertThat(port.ended).isFalse();

            port.mayEnd.set(true);
            exactLast.close();
            assertThat(port.attempted).containsExactly(physical, physical, physical);
            assertThat(port.ended).isTrue();
            exactLast.close();
            first.close();
            assertThat(port.attempted).as("successful logical releases are idempotent")
                    .containsExactly(physical, physical, physical);
        } finally {
            port.mayEnd.set(true);
            try { if (last != null) { last.close(); } }
            finally {
                try { if (first != null) { first.close(); } }
                finally { member.shutdown(); }
            }
        }
    }

    @Test
    void closingOneSharedSubscriberTwiceCannotStopItsOtherSubscriber() {
        HazelcastInstance member = emptyDurableMember();
        String key = "shared-subscriber-release-once";
        ControlledSharedClosePort port = new ControlledSharedClosePort(key);
        port.mayEnd.set(true);
        CaptureRun first = null, last = null;
        try {
            InMemoryMeta meta = new InMemoryMeta();
            CaptureRunUnit unit = new CaptureRunUnit(port, new SrsCoordinator(meta), meta, member);
            first = unit.start(specFor("release-first", ReadMode.CDC_ONLY, key), row -> { });
            last = unit.start(specFor("release-last", ReadMode.CDC_ONLY, key), row -> { });
            assertThat(first.chainId()).isEqualTo(last.chainId());
            assertThat(port.opened).hasSize(1);
            Subscription physical = port.opened.getFirst();

            first.close();
            first.close();
            assertThat(port.attempted).as("one subscriber cannot release the other subscriber's reference")
                    .isEmpty();
            assertThat(port.ended).isFalse();
            last.close();
            assertThat(port.attempted).containsExactly(physical);
            assertThat(port.ended).isTrue();
            first.close();
            last.close();
            assertThat(port.attempted).containsExactly(physical);
        } finally {
            port.mayEnd.set(true);
            try { if (last != null) { last.close(); } }
            finally {
                try { if (first != null) { first.close(); } }
                finally { member.shutdown(); }
            }
        }
    }

    /** Controlled native close; only the actual shared reference and retry protocol are under test. */
    private static final class ControlledSharedClosePort implements CapturePort {
        private final TapstateException refused;
        private final AtomicBoolean mayEnd = new AtomicBoolean();
        private final AtomicBoolean ended = new AtomicBoolean();
        private final List<Subscription> opened = new CopyOnWriteArrayList<>();
        private final List<Subscription> attempted = new CopyOnWriteArrayList<>();

        private ControlledSharedClosePort(String chain) {
            refused = new TapstateException(CaptureError.SRS_NOT_RECOVERABLE, Map.of("chain", chain), null);
        }

        @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            Subscription actual = new Subscription() {
                private boolean completed;
                @Override public synchronized void close() {
                    if (completed) { return; }
                    attempted.add(this);
                    if (!mayEnd.get()) { throw refused; }
                    completed = true;
                    ended.set(true);
                }
            };
            opened.add(actual);
            return actual;
        }
        @Override public CaptureBatch snapshot(CaptureConfig config) {
            throw new AssertionError("a CDC-only shared-close control must not open a snapshot");
        }
        @Override public ConnectionReport testConnection(CaptureConfig config) { throw new UnsupportedOperationException(); }
        @Override public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new UnsupportedOperationException(); }
    }


    @Test
    void configuredCloseCannotClaimLoadedWhileTheRealReservedWorkerIsStillOpeningItsTable() throws Exception {
        CountDownLatch reading = new CountDownLatch(1), release = new CountDownLatch(1);
        class Source implements CapturePort, SnapshotSession.Provider {
            @Override public SnapshotSession snapshotSession(CaptureConfig config) {
                return table -> {
                    reading.countDown(); awaitHeldShutdownRead(release);
                    return new FakeBatch(List.of(), "controlled-close-seam");
                };
            }
            @Override public CaptureBatch snapshot(CaptureConfig config) { throw new AssertionError("expected the real session seam"); }
            @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) { return () -> { }; }
            @Override public ConnectionReport testConnection(CaptureConfig config) { throw new UnsupportedOperationException(); }
            @Override public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new UnsupportedOperationException(); }
        }
        InMemoryMeta meta = new InMemoryMeta();
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1)) {
            CaptureRunUnit unit = new CaptureRunUnit(new Source(), new SrsCoordinator(meta), meta, hz,
                    new SnapshotBuffer(2, 1, 1_024, 512), workers);
            CaptureRun run = unit.begin(spec(ReadMode.SNAPSHOT_AND_CDC, false, "stubborn-close-read"), CaptureHandoff.of(row -> { }));
            try {
                run.activateSnapshot();
                assertThat(reading.await(5, TimeUnit.SECONDS)).isTrue();
                run.close();
                assertThat(run.awaitLoaded(Duration.ZERO))
                        .as("a returned close cannot forge completion of the actual reservation worker").isFalse();
                assertThat(run.loading()).isTrue();
            } finally {
                release.countDown();
                run.awaitLoaded(Duration.ofSeconds(5));
                run.close();
            }
        }
    }

    @Test
    void aLateNativeCloseFailureSurvivesTheCancelledReservedWorkersExit() throws Exception {
        CountDownLatch opening = new CountDownLatch(1), release = new CountDownLatch(1), closeAttempted = new CountDownLatch(1);
        AtomicInteger closes = new AtomicInteger();
        IllegalStateException original = new IllegalStateException("controlled late native close refusal");
        class Source implements CapturePort, SnapshotSession.Provider {
            @Override public SnapshotSession snapshotSession(CaptureConfig config) {
                return table -> new FakeBatch(List.of(), "late-close-seam");
            }
            @Override public CaptureBatch snapshot(CaptureConfig config) { throw new AssertionError("expected the real session seam"); }
            @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                opening.countDown(); awaitHeldShutdownRead(release);
                return () -> { closes.incrementAndGet(); closeAttempted.countDown(); throw original; };
            }
            @Override public ConnectionReport testConnection(CaptureConfig config) { throw new UnsupportedOperationException(); }
            @Override public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new UnsupportedOperationException(); }
        }
        InMemoryMeta meta = new InMemoryMeta();
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1)) {
            CaptureRunUnit unit = new CaptureRunUnit(new Source(), new SrsCoordinator(meta), meta, hz,
                    new SnapshotBuffer(2, 1, 1_024, 512), workers);
            CaptureRun run = unit.begin(spec(ReadMode.SNAPSHOT_AND_CDC, false, "late-native-close"), CaptureHandoff.of(row -> { }));
            try {
                run.activateSnapshot();
                assertThat(opening.await(5, TimeUnit.SECONDS)).isTrue();
                run.close();
                release.countDown();
                assertThat(closeAttempted.await(5, TimeUnit.SECONDS)).isTrue();
                workers.close();
                assertThat(closes).hasValue(1);
                assertThatThrownBy(() -> run.awaitLoaded(Duration.ZERO)).isSameAs(original);
                assertThatThrownBy(run::close).isSameAs(original);
                assertThat(run.failure()).as("requested cancellation is not a new business failure").isEmpty();
            } finally { release.countDown(); }
        }
    }

    private static void awaitHeldShutdownRead(CountDownLatch release) {
        boolean interrupted = false;
        try {
            while (release.getCount() != 0) {
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) { throw new AssertionError("the controlled native open was not released"); }
                } catch (InterruptedException ignored) { interrupted = true; }
            }
        } finally { if (interrupted) { Thread.currentThread().interrupt(); } }
    }

    @Test
    void aConfiguredDirectCdcOnlyReaderWaitsForItsExplicitAdmittedOwner() throws Exception {
        requireConfiguredCdcOnlyOwner(false);
    }

    @Test
    void aConfiguredDurableSharedCdcOnlyReaderWaitsForItsExplicitAdmittedOwner() throws Exception {
        requireConfiguredCdcOnlyOwner(true);
    }

    private void requireConfiguredCdcOnlyOwner(boolean shared) throws Exception {
        CaptureRunSpec request = spec(ReadMode.CDC_ONLY, shared,
                shared ? "admitted-shared-cdc-only" : "admitted-direct-cdc-only", StartFrom.latest());
        // Typed activation scopes isolate the runtime phase boundary; they do not assert native admission.
        LogSink.Scope admitted = new LogSink.Scope("cdc-only-resource", 7);
        record Opening(CapturePort reader, CaptureConfig config, PipelineNode node, LogSink.Scope owner,
                       AtomicInteger closeCalls) { }
        List<LogSink.Scope> views = new CopyOnWriteArrayList<>();
        List<Opening> opened = new CopyOnWriteArrayList<>();
        CountDownLatch tailOpened = new CountDownLatch(1);
        class Source implements CapturePort, LogScopedCapturePort {
            private final PipelineNode node;
            private final LogSink.Scope owner;
            Source(PipelineNode node, LogSink.Scope owner) { this.node = node; this.owner = owner; }
            @Override public CapturePort forLogOwner(PipelineNode actualNode, LogSink.Scope scope) {
                if (owner != null) { throw new AssertionError("a frozen source view cannot be rebound"); }
                assertThat(actualNode).isEqualTo(request.config().node());
                views.add(scope);
                return new Source(actualNode, scope);
            }
            @Override public CaptureBatch snapshot(CaptureConfig config) {
                throw new AssertionError("a CDC-only reader must not open a snapshot");
            }
            @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                AtomicInteger closes = new AtomicInteger();
                opened.add(new Opening(this, config, node, owner, closes));
                tailOpened.countDown();
                AtomicBoolean closed = new AtomicBoolean();
                return () -> { if (closed.compareAndSet(false, true)) { closes.incrementAndGet(); } };
            }
            @Override public ConnectionReport testConnection(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }
            @Override public DiscoveredSchema discoverSchema(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }
        }
        InMemoryMeta meta = new InMemoryMeta();
        SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 1_024, 512);
        HazelcastInstance member = emptyDurableMember();
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1);
             CaptureRunUnit unit = new CaptureRunUnit(new Source(null, null), new SrsCoordinator(meta), meta,
                     member, buffer, workers)) {
            try (CaptureRun run = unit.begin(request, CaptureHandoff.of(row -> { }))) {
                assertThat(views).as("preparation has no explicit admitted log owner").isEmpty();
                assertThat(opened).as("the source must not open before the consuming execution activates it").isEmpty();

                run.activateSnapshot(admitted);
                assertThat(tailOpened.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(run.failure()).isEmpty();
                assertThat(views).containsExactly(admitted);
                assertThat(opened).hasSize(1);
                Opening actual = opened.getFirst();
                assertThat(actual.node()).isEqualTo(request.config().node());
                assertThat(actual.config().node()).isEqualTo(request.config().node());
                assertThat(actual.owner()).isEqualTo(admitted);
                assertThat(actual.reader()).isNotNull();
                assertThat(actual.config().streams()).containsExactlyElementsOf(request.config().streams());
                assertThat(actual.config().sharedNotes() != null).isEqualTo(shared);
                assertThat(actual.closeCalls()).hasValue(0);

                run.activateSnapshot(admitted);
                assertThatThrownBy(() -> run.activateSnapshot(new LogSink.Scope("cdc-only-resource", 8)))
                        .isInstanceOf(IllegalStateException.class);
                assertThatThrownBy(() -> run.activateSnapshot(new LogSink.Scope("other-resource", 7)))
                        .isInstanceOf(IllegalStateException.class);
                assertThat(views).containsExactly(admitted);
                assertThat(opened).containsExactly(actual);
                run.close();
                run.close();
                assertThat(actual.closeCalls()).hasValue(1);
            }
        } finally { member.shutdown(); }
    }

    @Test
    void aConfiguredCdcOnlyCloseBeforeActivationReleasesCapacityWithoutOpeningANativeReader() throws Exception {
        AtomicInteger opens = new AtomicInteger();
        CapturePort source = deferredCdcOnlyPort(() -> { opens.incrementAndGet(); return () -> { }; });
        InMemoryMeta meta = new InMemoryMeta();
        SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 1_024, 512);
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1);
             CaptureRunUnit unit = new CaptureRunUnit(source, new SrsCoordinator(meta), meta, hz, buffer, workers);
             CaptureRun run = unit.begin(spec(ReadMode.CDC_ONLY, false, "closed-cdc-only"), CaptureHandoff.of(row -> { }))) {
            assertThat(opens).hasValue(0);
            run.abandonLoad();
            assertThat(opens).as("abandoning a nonexistent snapshot cannot bypass execution activation").hasValue(0);
            run.close();
            run.close();
            run.activateSnapshot(new LogSink.Scope("closed-cdc-resource", 7));
            assertThat(run.awaitLoaded(Duration.ZERO)).isTrue();
            assertThat(run.loading()).isFalse();
            assertThat(run.snapshotCount()).isZero();
            assertThat(run.snapshotCounts()).isEmpty();
            assertThat(run.failure()).isEmpty();
            assertThat(opens).hasValue(0);
            try (SnapshotWorkers.Reservation first = workers.reserve().orElseThrow();
                 SnapshotWorkers.Reservation second = workers.reserve().orElseThrow()) {
                assertThat(workers.reserve()).as("both original capacity permits are available, with no extra one").isEmpty();
            }
        }
    }

    @Test
    void anExplicitUnscopedCdcOnlyActivationOpensTheOriginalReaderWithoutInventingALogOwner() throws Exception {
        CaptureRunSpec request = spec(ReadMode.CDC_ONLY, false, "explicit-unscoped-cdc-only");
        AtomicInteger opens = new AtomicInteger(), closes = new AtomicInteger(), scopedViews = new AtomicInteger();
        AtomicReference<CapturePort> actualReader = new AtomicReference<>();
        CountDownLatch opened = new CountDownLatch(1);
        class Source implements CapturePort, LogScopedCapturePort {
            @Override public CapturePort forLogOwner(PipelineNode node, LogSink.Scope scope) {
                scopedViews.incrementAndGet();
                throw new AssertionError("an unscoped activation cannot invent an admitted log owner");
            }
            @Override public CaptureBatch snapshot(CaptureConfig config) {
                throw new AssertionError("a CDC-only activation cannot open a snapshot");
            }
            @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                assertThat(config.node()).isEqualTo(request.config().node());
                opens.incrementAndGet();
                actualReader.set(this);
                opened.countDown();
                AtomicBoolean ended = new AtomicBoolean();
                return () -> { if (ended.compareAndSet(false, true)) { closes.incrementAndGet(); } };
            }
            @Override public ConnectionReport testConnection(CaptureConfig config) { throw new UnsupportedOperationException(); }
            @Override public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new UnsupportedOperationException(); }
        }
        Source original = new Source();
        InMemoryMeta meta = new InMemoryMeta();
        SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 1_024, 512);
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1);
             CaptureRunUnit unit = new CaptureRunUnit(original, new SrsCoordinator(meta), meta, hz, buffer, workers);
             CaptureRun run = unit.begin(request, CaptureHandoff.of(row -> { }))) {
            assertThat(opens).as("begin has not activated the native reader").hasValue(0);
            assertThat(scopedViews).hasValue(0);
            run.activateSnapshot();
            assertThat(opened.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(run.awaitLoaded(Duration.ofSeconds(5))).isTrue();
            assertThat(actualReader).hasValue(original);
            assertThat(opens).hasValue(1);
            assertThat(scopedViews).hasValue(0);
            assertThat(run.failure()).isEmpty();
            assertThat(run.snapshotCount()).isZero();
            assertThat(run.snapshotCounts()).isEmpty();
            run.activateSnapshot();
            // This typed fixture scope checks immutability; it is not an actual native admission receipt.
            assertThatThrownBy(() -> run.activateSnapshot(new LogSink.Scope("no-retroactive-log-owner", 7)))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(opens).hasValue(1);
            assertThat(scopedViews).hasValue(0);
            run.close();
            run.close();
            assertThat(closes).hasValue(1);
        }
    }

    @Test
    void configuredCdcOnlyPreparationsUseTheExistingFiniteWorkerReservationBudget() {
        AtomicInteger opens = new AtomicInteger();
        CapturePort source = deferredCdcOnlyPort(() -> { opens.incrementAndGet(); return () -> { }; });
        InMemoryMeta meta = new InMemoryMeta();
        SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 1_024, 512);
        CaptureRunSpec third = spec(ReadMode.CDC_ONLY, false, "capacity-cdc-three");
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1);
             CaptureRunUnit unit = new CaptureRunUnit(source, new SrsCoordinator(meta), meta, hz, buffer, workers);
             CaptureRun first = unit.begin(spec(ReadMode.CDC_ONLY, false, "capacity-cdc-one"), CaptureHandoff.of(row -> { }));
             CaptureRun second = unit.begin(spec(ReadMode.CDC_ONLY, false, "capacity-cdc-two"), CaptureHandoff.of(row -> { }))) {
            assertThatThrownBy(() -> unit.begin(third, CaptureHandoff.of(row -> { })))
                    .isInstanceOf(SnapshotCapacityUnavailable.class);
            assertThat(meta.read(third.miningChainId().value())).isEmpty();
            assertThat(opens).hasValue(0);
            first.close();
            try (CaptureRun admittedLater = unit.begin(third, CaptureHandoff.of(row -> { }))) {
                assertThat(admittedLater.snapshotCount()).isZero();
                assertThat(opens).hasValue(0);
            }
        }
    }

    @Test
    void aClosedConfiguredCdcOnlyRunKeepsItsLateExactNativeCloseFailureUntilRetry() throws Exception {
        CountDownLatch opening = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger opens = new AtomicInteger(), closes = new AtomicInteger();
        AtomicBoolean mayEnd = new AtomicBoolean(), ended = new AtomicBoolean();
        CaptureRunSpec request = spec(ReadMode.CDC_ONLY, false, "late-cdc-only-native-close");
        TapstateException original = new TapstateException(CaptureError.SRS_NOT_RECOVERABLE,
                Map.of("chain", request.miningChainId().value()), null);
        Subscription exact = () -> {
            if (ended.get()) { return; }
            closes.incrementAndGet();
            if (!mayEnd.get()) { throw original; }
            ended.set(true);
        };
        CapturePort source = deferredCdcOnlyPort(() -> {
            opens.incrementAndGet();
            opening.countDown();
            awaitHeldShutdownRead(release);
            return exact;
        });
        InMemoryMeta meta = new InMemoryMeta();
        SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 1_024, 512);
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1);
             CaptureRunUnit unit = new CaptureRunUnit(source, new SrsCoordinator(meta), meta, hz, buffer, workers);
             CaptureRun run = unit.begin(request, CaptureHandoff.of(row -> { }))) {
            try {
                // A typed fixture scope isolates activation and teardown, not a claim or native admission.
                run.activateSnapshot(new LogSink.Scope("late-cdc-resource", 7));
                assertThat(opening.await(5, TimeUnit.SECONDS)).isTrue();
                run.close();
                assertThat(run.loading()).as("a cancelled reservation is not an ended native opening").isTrue();
                assertThat(run.awaitLoaded(Duration.ZERO)).isFalse();
                release.countDown();
                assertThatThrownBy(() -> run.awaitLoaded(Duration.ofSeconds(5))).isSameAs(original);
                assertThat(opens).hasValue(1);
                assertThat(closes).hasValue(1);
                assertThat(ended).isFalse();
                assertThat(run.failure()).as("requested cancellation does not create a business failure").isEmpty();
                mayEnd.set(true);
                run.close();
                assertThat(run.awaitLoaded(Duration.ZERO)).isTrue();
                assertThat(ended).isTrue();
                assertThat(closes).hasValue(2);
                run.close();
                assertThat(closes).hasValue(2);
            } finally { mayEnd.set(true); release.countDown(); }
        }
    }

    @Test
    void aConfiguredRemoteCdcOnlyAttachmentCannotOpenItsPhysicalTail() {
        HazelcastInstance member = emptyDurableMember();
        String key = "remote-deferred-cdc-only";
        AtomicInteger remoteOpens = new AtomicInteger();
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource actualOwner = new FakeSource(List.of(), List.of());
        CapturePort remote = deferredCdcOnlyPort(() -> { remoteOpens.incrementAndGet(); return () -> { }; });
        SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 1_024, 512);
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1);
             CaptureRunUnit owner = new CaptureRunUnit(actualOwner, new SrsCoordinator(meta), meta, member);
             CaptureRun first = owner.start(specFor("owner-cdc-only", ReadMode.CDC_ONLY, key), row -> { });
             CaptureRunUnit joining = new CaptureRunUnit(remote, new SrsCoordinator(meta), meta, member, buffer, workers);
             CaptureRun attached = joining.begin(specFor("remote-cdc-only", ReadMode.CDC_ONLY, key),
                     CaptureHandoff.of(row -> { }), false)) {
            attached.activateSnapshot(new LogSink.Scope("remote-cdc-resource", 7));
            assertThat(attached.chainId()).isEqualTo(first.chainId());
            assertThat(attached.cdcSubscription()).isEmpty();
            assertThat(remoteOpens).hasValue(0);
            assertThat(actualOwner.cdcStarts).isOne();
            assertThat(attached.snapshotCount()).isZero();
            try (SnapshotWorkers.Reservation one = workers.reserve().orElseThrow();
                 SnapshotWorkers.Reservation two = workers.reserve().orElseThrow()) {
                assertThat(workers.reserve()).isEmpty();
            }
        } finally { member.shutdown(); }
    }

    private static CapturePort deferredCdcOnlyPort(Supplier<Subscription> tail) {
        return new CapturePort() {
            @Override public CaptureBatch snapshot(CaptureConfig config) {
                throw new AssertionError("a CDC-only activation cannot open a snapshot");
            }
            @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                return tail.get();
            }
            @Override public ConnectionReport testConnection(CaptureConfig config) { throw new UnsupportedOperationException(); }
            @Override public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new UnsupportedOperationException(); }
        };
    }

    @Test
    void anActivatedReaderFreezesOneExplicitOwnerForItsSnapshotAndInitialTail() throws Exception {
        CaptureRunSpec request = spec(ReadMode.SNAPSHOT_AND_CDC, false, "explicit-log-owner")
                .withSnapshotWriterToken("explicit-owner-run");
        LogSink.Scope admitted = new LogSink.Scope("resource-a", 7);
        List<LogSink.Scope> views = new CopyOnWriteArrayList<>();
        List<LogSink.Scope> snapshotOwners = new CopyOnWriteArrayList<>();
        List<LogSink.Scope> tailOwners = new CopyOnWriteArrayList<>();
        class Source implements CapturePort, SnapshotSession.Provider, LogScopedCapturePort {
            private final LogSink.Scope owner;
            Source(LogSink.Scope owner) { this.owner = owner; }
            @Override public CapturePort forLogOwner(PipelineNode node, LogSink.Scope scope) {
                assertThat(node).isEqualTo(request.config().node());
                views.add(scope);
                return new Source(scope);
            }
            @Override public SnapshotSession snapshotSession(CaptureConfig config) {
                assertThat(owner).as("no source handle may open before the explicit owner arrives").isEqualTo(admitted);
                snapshotOwners.add(owner);
                return table -> new FakeBatch(List.of(), "explicit-owner-seam");
            }
            @Override public CaptureBatch snapshot(CaptureConfig config) {
                throw new AssertionError("the shared session must retain its scoped port view");
            }
            @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                assertThat(owner).isEqualTo(admitted);
                tailOwners.add(owner);
                return () -> { };
            }
            @Override public ConnectionReport testConnection(CaptureConfig config) { throw new UnsupportedOperationException(); }
            @Override public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new UnsupportedOperationException(); }
        }
        InMemoryMeta meta = new InMemoryMeta();
        SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 1_024, 512);
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1)) {
            CaptureRunUnit unit = new CaptureRunUnit(new Source(null), new SrsCoordinator(meta), meta, hz, buffer, workers);
            try (CaptureRun run = unit.begin(request, CaptureHandoff.of(row -> { }))) {
                assertThat(views).isEmpty();
                assertThat(snapshotOwners).isEmpty();
                assertThat(tailOwners).isEmpty();
                run.activateSnapshot(admitted);
                assertThat(run.awaitLoaded(Duration.ofSeconds(5))).isTrue();
                assertThat(run.failure()).isEmpty();
                assertThat(views).containsExactly(admitted);
                assertThat(snapshotOwners).containsExactly(admitted);
                assertThat(tailOwners).containsExactly(admitted);
                run.activateSnapshot(admitted);
                assertThatThrownBy(() -> run.activateSnapshot(new LogSink.Scope("resource-a", 8)))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("an activated capture cannot change its admitted log owner");
                assertThat(views).containsExactly(admitted);
                assertThat(snapshotOwners).containsExactly(admitted);
                assertThat(tailOwners).containsExactly(admitted);
            }
        }
    }

    @Test
    void aDurableSharedTailKeepsItsFirstScopedReaderWhenAnotherConsumerWidensIt() throws Exception {
        String key = "shared-tail-log-owner";
        CaptureConfig originalConfig = new CaptureConfig("demo", Map.of("host", "h"), List.of("orders"));
        CaptureConfig joiningConfig = new CaptureConfig("demo", Map.of("host", "h"), List.of("customers"));
        CaptureRunSpec original = new CaptureRunSpec(originalConfig, ReadMode.SNAPSHOT_AND_CDC, key, true,
                "original_source", "original_pipeline", StartFrom.latest(), null, 0L)
                .withConsumerId(SrsConsumerId.of("original_pipeline", "original_source").value())
                .withSnapshotWriterToken("original-load");
        CaptureRunSpec joining = new CaptureRunSpec(joiningConfig, ReadMode.SNAPSHOT_AND_CDC, key, true,
                "joining_source", "joining_pipeline", StartFrom.latest(), null, 0L)
                .withConsumerId(SrsConsumerId.of("joining_pipeline", "joining_source").value())
                .withSnapshotWriterToken("joining-load");
        assertThat(joining.miningChainId()).isEqualTo(original.miningChainId());
        // Typed activation scopes exercise handle attribution, not native execution admission.
        LogSink.Scope originalScope = new LogSink.Scope("original-resource", 7);
        LogSink.Scope joiningScope = new LogSink.Scope("joining-resource", 19);
        record Opening(CapturePort reader, PipelineNode node, LogSink.Scope scope,
                       List<String> tables, AtomicInteger closeCalls) { }
        List<Opening> snapshots = new CopyOnWriteArrayList<>();
        List<Opening> physicalTails = new CopyOnWriteArrayList<>();
        CountDownLatch firstTail = new CountDownLatch(1), widenedTail = new CountDownLatch(1);
        class Source implements CapturePort, SnapshotSession.Provider, LogScopedCapturePort {
            private final PipelineNode node;
            private final LogSink.Scope scope;
            Source(PipelineNode node, LogSink.Scope scope) { this.node = node; this.scope = scope; }
            @Override public CapturePort forLogOwner(PipelineNode owner, LogSink.Scope admitted) {
                if (scope != null) { throw new AssertionError("a frozen reader cannot be rebound"); }
                return new Source(owner, admitted);
            }
            @Override public SnapshotSession snapshotSession(CaptureConfig config) {
                assertThat(node).isNotNull().isEqualTo(config.node());
                assertThat(scope).isNotNull();
                snapshots.add(new Opening(this, node, scope, config.streams(), new AtomicInteger()));
                return table -> new FakeBatch(List.of(), "seam-" + node.nodeId());
            }
            @Override public CaptureBatch snapshot(CaptureConfig config) {
                throw new AssertionError("the scoped snapshot must use its actual shared session");
            }
            @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                assertThat(node).isNotNull().isEqualTo(config.node());
                assertThat(scope).isNotNull();
                assertThat(config.sharedNotes().sharedBy()).isEqualTo(original.miningChainId().value());
                AtomicInteger closeCalls = new AtomicInteger();
                physicalTails.add(new Opening(this, config.node(), scope, config.streams(), closeCalls));
                if (physicalTails.size() == 1) { firstTail.countDown(); }
                if (physicalTails.size() == 2) { widenedTail.countDown(); }
                AtomicBoolean closed = new AtomicBoolean();
                return () -> { if (closed.compareAndSet(false, true)) { closeCalls.incrementAndGet(); } };
            }
            @Override public ConnectionReport testConnection(CaptureConfig config) { throw new UnsupportedOperationException(); }
            @Override public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new UnsupportedOperationException(); }
        }
        class SelectingMeta extends InMemoryMeta {
            private final Map<String, List<String>> requested = new LinkedHashMap<>();
            @Override public synchronized void requestCaptureTables(String chain, List<String> tables) {
                java.util.LinkedHashSet<String> union = new java.util.LinkedHashSet<>(
                        requested.getOrDefault(chain, List.of()));
                union.addAll(tables);
                requested.put(chain, List.copyOf(union));
            }
            @Override public synchronized List<String> captureTables(String chain) {
                return requested.getOrDefault(chain, List.of());
            }
            @Override public synchronized boolean publishCaptureTables(String chain, long epoch, List<String> tables) {
                return read(chain).filter(record -> record.epoch() == epoch).isPresent()
                        && tables.containsAll(captureTables(chain));
            }
        }
        HazelcastInstance member = emptyDurableMember();
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1)) {
            SelectingMeta meta = new SelectingMeta();
            SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 1_024, 512);
            CaptureRunUnit unit = new CaptureRunUnit(new Source(null, null), new SrsCoordinator(meta), meta,
                    member, buffer, workers);
            try (CaptureRun first = unit.begin(original, CaptureHandoff.of(row -> { }))) {
                assertThat(physicalTails).isEmpty();
                assertThat(snapshots).isEmpty();
                first.activateSnapshot(originalScope);
                assertThat(firstTail.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(first.awaitLoaded(Duration.ofSeconds(5))).isTrue();
                assertThat(first.failure()).isEmpty();
                assertThat(physicalTails).hasSize(1);
                Opening initial = physicalTails.getFirst();
                assertThat(initial.reader()).isSameAs(snapshots.getFirst().reader());
                assertThat(initial.node()).isEqualTo(original.config().node());
                assertThat(initial.scope()).isEqualTo(originalScope);
                assertThat(initial.tables()).containsExactly("orders");
                try (CaptureRun second = unit.begin(joining, CaptureHandoff.of(row -> { }))) {
                    assertThat(second.merged()).isTrue();
                    assertThat(meta.consumerOffsets(original.miningChainId().value()))
                            .extracting(ConsumerOffset::pipelineId)
                            .containsExactlyInAnyOrder(original.consumerId(), joining.consumerId());
                    assertThat(physicalTails).hasSize(1);
                    second.activateSnapshot(joiningScope);
                    assertThat(widenedTail.await(5, TimeUnit.SECONDS)).isTrue();
                    assertThat(second.awaitLoaded(Duration.ofSeconds(5))).isTrue();
                    assertThat(second.failure()).isEmpty();
                    assertThat(first.failure()).isEmpty();
                    assertThat(snapshots).hasSize(2);
                    Opening joiningSnapshot = snapshots.getLast();
                    assertThat(joiningSnapshot.node()).isEqualTo(joining.config().node());
                    assertThat(joiningSnapshot.scope()).isEqualTo(joiningScope);
                    assertThat(joiningSnapshot.reader()).isNotSameAs(initial.reader());
                    assertThat(physicalTails).hasSize(2);
                    Opening reopened = physicalTails.getLast();
                    assertThat(initial.closeCalls()).hasValue(1);
                    assertThat(reopened.reader()).isSameAs(initial.reader());
                    assertThat(reopened.node()).isEqualTo(original.config().node())
                            .isNotEqualTo(joining.config().node());
                    assertThat(reopened.scope()).isEqualTo(originalScope).isNotEqualTo(joiningScope);
                    assertThat(reopened.tables()).containsExactlyInAnyOrder("orders", "customers");
                    assertThat(reopened.closeCalls()).hasValue(0);
                    first.close();
                    assertThat(reopened.closeCalls()).as("the joined consumer still holds the original physical reader")
                            .hasValue(0);
                }
                assertThat(physicalTails.getLast().closeCalls()).hasValue(1);
            }
        } finally { member.shutdown(); }
    }

    @Test
    void reservedSnapshotReturnsBeforeReadingAndStartsOnlyAfterActivation() throws Exception {
        CountDownLatch firstRow = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        CapturePort source = new CapturePort() {
            @Override public CaptureBatch snapshot(CaptureConfig config) {
                throw new AssertionError("whole snapshot batch must not be used");
            }
            @Override public void streamSnapshot(CaptureConfig config, SnapshotListener listener) {
                listener.seam(Optional.empty());
                listener.row(row(1));
                firstRow.countDown();
                try {
                    if (!releaseRead.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("snapshot read was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                listener.row(row(2));
            }
            @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                throw new AssertionError("snapshot-only capture has no CDC tail");
            }
            @Override public ConnectionReport testConnection(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }
            @Override public DiscoveredSchema discoverSchema(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }
        };
        SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 1_024, 512);
        InMemoryMeta meta = new InMemoryMeta();
        String ringName = SrsRingbuffer.ringName(
                spec(ReadMode.SNAPSHOT_ONLY, false, "deferred-snapshot-test").miningChainId().value(), "orders");
        List<Envelope> observed = new java.util.concurrent.CopyOnWriteArrayList<>();
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1);
                var caller = Executors.newSingleThreadExecutor()) {
            CaptureRunUnit unit = new CaptureRunUnit(source, new SrsCoordinator(meta), meta, hz,
                    buffer, workers);
            CaptureRunSpec request = spec(ReadMode.SNAPSHOT_ONLY, false, "deferred-snapshot-test")
                    .withSnapshotWriterToken("run-a");
            CaptureRun run = caller.submit(() -> unit.start(request, observed::add)).get(5, TimeUnit.SECONDS);
            try {
                assertThat(firstRow.getCount()).isEqualTo(1);
                assertThat(buffer.hasSnapshot("pipe-1", ringName, "run-a")).isTrue();
                assertThat(buffer.drainSnapshot("pipe-1", ringName,
                        request.snapshotWriterToken(), 1).state())
                        .isEqualTo(SnapshotBuffer.SessionState.ACTIVE);
                run.activateSnapshot();
                assertThat(firstRow.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(observed).hasSize(1);
                List<Envelope> buffered = new ArrayList<>(
                        buffer.drainSnapshot("pipe-1", ringName, "run-a", 1).rows());
                releaseRead.countDown();
                SnapshotBuffer.SessionState terminal = SnapshotBuffer.SessionState.ACTIVE;
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (terminal != SnapshotBuffer.SessionState.DONE && System.nanoTime() < until) {
                    SnapshotBuffer.SessionDrain next = buffer.drainSnapshot("pipe-1", ringName, "run-a", 1);
                    buffered.addAll(next.rows());
                    terminal = next.state();
                    if (terminal == SnapshotBuffer.SessionState.ACTIVE) {
                        Thread.sleep(5);
                    }
                }
                assertThat(terminal).isEqualTo(SnapshotBuffer.SessionState.DONE);
                assertThat(buffered).extracting(event -> event.after().get("id")).containsExactly(1, 2);
                assertThat(observed).hasSize(2);
            } finally {
                releaseRead.countDown();
                run.close();
            }
        }
    }

    @Test
    void aReservedTailFailureAfterLoadAbandonmentIsStillReported() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        IllegalStateException refused = new IllegalStateException("the source refused the tail after load abandonment");
        LoadThenTailSource source = new LoadThenTailSource(List.of(row(1), row(2)), () -> { throw refused; });
        CountDownLatch entered = new CountDownLatch(1), released = new CountDownLatch(1);
        CaptureHandoff handoff = new CaptureHandoff() {
            @Override public void accept(Envelope row) {
                entered.countDown();
                awaitRoom(released);
                throw new CancellationException("the load was explicitly abandoned");
            }
            @Override public void loaded(String table) { throw new AssertionError("an abandoned table is not complete"); }
        };
        SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 1_024, 512);
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1)) {
            CaptureRunUnit unit = new CaptureRunUnit(source, new SrsCoordinator(meta), meta, hz, buffer, workers);
            CaptureRun run = unit.begin(spec(ReadMode.SNAPSHOT_AND_CDC, false, "reserved-abandoned-tail"), handoff);
            try {
                run.activateSnapshot();
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                run.abandonLoad();
                released.countDown();
                assertThat(run.awaitLoaded(Duration.ofSeconds(10))).isTrue();
                assertThat(run.failure()).containsSame(refused);
            } finally {
                released.countDown();
                run.close();
            }
        }
    }

    @Test
    void aVolatileCompatibilityTailTreatsAnExplicitUnreadSubscriptionAsHavingReadNothing() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-other", null);
        meta.openEpoch("chain-other");
        // The consumer has a cursor on another selected table but none on orders. Its orders selection
        // still protects the ring's first change.
        meta.advanceConsumerReadSeq("chain-other", "p1", "orders", -1L);
        meta.advanceConsumerReadSeq("chain-other", "p1", "customers", 9L);

        assertThat(CdcPhase.headroomBound(meta.consumerOffsets("chain-other"), "orders")).isEqualTo(-1L);
    }

    @Test
    void aConfiguredChainedSnapshotDoesNotOpenItsFirstTableBeforeActivation() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        AtomicBoolean activated = new AtomicBoolean();
        CountDownLatch reading = new CountDownLatch(1), releaseRead = new CountDownLatch(1);
        class Source implements CapturePort, SnapshotSession.Provider {
            @Override public SnapshotSession snapshotSession(CaptureConfig config) {
                return new SnapshotSession() {
                    @Override public CaptureBatch read(String table) {
                        reads.incrementAndGet();
                        reading.countDown();
                        if (activated.get()) {
                            try {
                                if (!releaseRead.await(5, TimeUnit.SECONDS)) {
                                    throw new AssertionError("the activated first-table read was not released");
                                }
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException("the first-table read was interrupted", interrupted);
                            }
                        }
                        return new FakeBatch(List.of(row(1)), "controlled-first-table-seam");
                    }
                    @Override public void close() { }
                };
            }
            @Override public CaptureBatch snapshot(CaptureConfig config) {
                throw new AssertionError("the chained source must use its actual snapshot session");
            }
            @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                return () -> { };
            }
            @Override public ConnectionReport testConnection(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }
            @Override public DiscoveredSchema discoverSchema(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }
        }
        InMemoryMeta meta = new InMemoryMeta();
        SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 1_024, 512);
        CaptureRunSpec request = spec(ReadMode.SNAPSHOT_AND_CDC, false, "activated-chained-snapshot")
                .withSnapshotWriterToken("activated-run");
        String ringName = SrsRingbuffer.ringName(request.miningChainId().value(), "orders");
        List<Envelope> observed = new CopyOnWriteArrayList<>();
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1);
                var caller = Executors.newSingleThreadExecutor()) {
            CaptureRunUnit unit = new CaptureRunUnit(new Source(), new SrsCoordinator(meta), meta, hz, buffer, workers);
            CaptureRun run = caller.submit(() -> unit.begin(request, CaptureHandoff.of(observed::add))).get(5, TimeUnit.SECONDS);
            try {
                assertThat(reads).as("provisioning cannot read a table before its real consumer activates").hasValue(0);
                assertThat(buffer.hasSnapshot(request.pipelineId(), ringName, request.snapshotWriterToken())).isTrue();
                activated.set(true);
                run.activateSnapshot();
                assertThat(reading.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(reads).hasValue(1);
                assertThat(observed).isEmpty();
                releaseRead.countDown();
                assertThat(run.awaitLoaded(Duration.ofSeconds(5))).isTrue();
                assertThat(run.failure()).isEmpty();
                assertThat(observed).hasSize(1);
                assertThat(buffer.drainSnapshot(request.pipelineId(), ringName, request.snapshotWriterToken(), 2).state())
                        .isEqualTo(SnapshotBuffer.SessionState.DONE);
            } finally {
                releaseRead.countDown();
                run.close();
            }
        }
    }

    @Test
    void closingAQueuedCompatibilityLoadReleasesItsSlotWithoutInterruptingTheRunningLoad() throws Exception {
        CountDownLatch reading = new CountDownLatch(1), releaseRead = new CountDownLatch(1);
        AtomicReference<Thread> firstWorker = new AtomicReference<>(), nextWorker = new AtomicReference<>();
        AtomicInteger tails = new AtomicInteger();
        CaptureHealth firstHealth = new CaptureHealth(), queuedHealth = new CaptureHealth(), nextHealth = new CaptureHealth();
        List<Envelope> firstRows = new CopyOnWriteArrayList<>(), queuedRows = new CopyOnWriteArrayList<>(),
                nextRows = new CopyOnWriteArrayList<>();
        Supplier<Optional<Subscription>> tail = () -> {
            tails.incrementAndGet();
            return Optional.empty();
        };
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1)) {
            SnapshotPhase.Load firstLoad = SnapshotPhase.openChainless(
                    new FakeSource(List.of(row(1)), List.of()), config(), 1);
            BackgroundLoad first = new BackgroundLoad(firstLoad, CaptureHandoff.of(event -> {
                firstWorker.set(Thread.currentThread());
                reading.countDown();
                awaitRoom(releaseRead);
                firstRows.add(event);
            }), tail, firstHealth, workers.reserve().orElseThrow());
            SnapshotPhase.Load queuedLoad = SnapshotPhase.openChainless(
                    new FakeSource(List.of(row(2)), List.of()), config(), 2);
            BackgroundLoad queued = new BackgroundLoad(queuedLoad, CaptureHandoff.of(queuedRows::add),
                    tail, queuedHealth, workers.reserve().orElseThrow());
            BackgroundLoad next = null;
            try {
                first.start();
                assertThat(reading.await(5, TimeUnit.SECONDS)).isTrue();
                queued.start();
                assertThat(workers.reserve()).as("the running and queued loads occupy the bounded capacity").isEmpty();

                queued.cancel();

                assertThat(queued.awaitFinished(Duration.ofSeconds(5))).isTrue();
                assertThat(queuedRows).isEmpty();
                assertThat(queuedHealth.failure()).isEmpty();
                assertThat(tails).hasValue(0);
                SnapshotWorkers.Reservation replacement = workers.reserve().orElseThrow();
                SnapshotPhase.Load nextLoad = SnapshotPhase.openChainless(
                        new FakeSource(List.of(row(3)), List.of()), config(), 3);
                next = new BackgroundLoad(nextLoad, CaptureHandoff.of(event -> {
                    nextWorker.set(Thread.currentThread());
                    nextRows.add(event);
                }), tail, nextHealth, replacement);
                next.start();
                assertThat(first.awaitFinished(Duration.ZERO)).isFalse();
                releaseRead.countDown();

                assertThat(first.awaitFinished(Duration.ofSeconds(5))).isTrue();
                assertThat(next.awaitFinished(Duration.ofSeconds(5))).isTrue();
                assertThat(firstHealth.failure()).isEmpty();
                assertThat(nextHealth.failure()).isEmpty();
                assertThat(firstRows).extracting(event -> event.after().get("id")).containsExactly(1);
                assertThat(nextRows).extracting(event -> event.after().get("id")).containsExactly(3);
                assertThat(nextWorker.get()).isSameAs(firstWorker.get());
                assertThat(tails).hasValue(2);
            } finally {
                releaseRead.countDown();
                queued.cancel();
                first.cancel();
                if (next != null) {
                    next.cancel();
                }
            }
        }
    }

    private static HazelcastInstance hz;

    @BeforeAll
    static void startMember() {
        Config config = new Config();
        // Isolated, structurally undiscoverable single member -- never merge with anything on the LAN.
        config.setClusterName("srs-rununit-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(false);
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(8)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setBackupCount(0));
        config.getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setImplementation(new SrsItemSerializer()).setTypeClass(SrsItem.class));
        hz = Hazelcast.newHazelcastInstance(config);
    }

    @AfterAll
    static void stopMember() {
        if (hz != null) {
            hz.shutdown();
        }
    }

    private static CaptureConfig config() {
        return new CaptureConfig("mysql", Map.of("host", "h"), List.of("orders"));
    }

    private static Envelope row(int id) {
        return Envelope.read(id, "orders", Map.of("id", id), Map.of());
    }

    private static Envelope change(int id) {
        return Envelope.insert(id, "orders", Map.of("id", id), Map.of());
    }

    @Test
    void aBoundLogCannotCertifyCaptureThroughAMemoryOnlyRing() {
        SrsLogStore unusedLog = new SrsLogStore() {
            @Override
            public void store(String ring, long sequence, SrsLogRecord record) {
                throw new AssertionError("unsafe capture must stop before writing any change");
            }

            @Override
            public void storeAll(String ring, long sequence, List<SrsLogRecord> records) {
                throw new AssertionError("unsafe capture must stop before writing a batch");
            }

            @Override
            public Optional<SrsLogRecord> load(String ring, long sequence) {
                throw new AssertionError("a memory-only ring cannot recover from the bound log");
            }

            @Override
            public long largestSequence(String ring) {
                throw new AssertionError("unsafe capture must stop before sampling a durable arrival");
            }

            @Override
            public void trim(String ring, long sequence) {
                throw new AssertionError("unsafe capture must not trim any history");
            }
        };
        hz.getUserContext().put(CaptureRunUnit.SRS_LOG_USER_CONTEXT_KEY, unusedLog);
        try {
            for (ReadMode mode : List.of(ReadMode.SNAPSHOT_AND_CDC, ReadMode.CDC_ONLY)) {
                FakeSource source = new FakeSource(List.of(row(1)), List.of(change(2)));
                List<Envelope> handedOff = new ArrayList<>();

                TapstateException failure = catchThrowableOfType(() -> runUnit(source, new InMemoryMeta())
                        .start(spec(mode, true, "memory-only-" + mode), handedOff::add), TapstateException.class);

                assertThat(failure.code()).isEqualTo(CaptureError.SRS_NOT_RECOVERABLE);
                assertThat(source.cdcStarted).isFalse();
                assertThat(handedOff).isEmpty();
            }
        } finally {
            hz.getUserContext().remove(CaptureRunUnit.SRS_LOG_USER_CONTEXT_KEY);
        }
    }


    /**
     * Turning the buffer off between two runs does not lose where the first one got to.
     *
     * <p>What the switch decides is whether changes are staged for replay. Where a run begins is decided
     * by the record, and the record belongs to the chain rather than to the buffer -- so a run that reads
     * its source directly picks up exactly where a buffered one stopped. When the accounting was kept with
     * the buffer instead, flipping the switch threw the position away with it and the next run began at the
     * source's present moment: every change made in between simply never arrived.
     *
     * <p>The position is read back out of the record rather than written into the assertion. What it is
     * exactly is the chain's business; what this case is about is that the second run is handed the same
     * one, and spelling a token here would make the case fail the day that spelling changes for a reason
     * nobody cares about.
     */
    @Test
    void aRunThatTurnsTheBufferOffBeginsWhereTheBufferedOneStopped() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource buffered = new FakeSource(List.of(row(1), row(2)), List.of(change(10)));
        CaptureRun first = runUnit(buffered, meta)
                .start(spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-flip-off"), e -> { });
        String chain = first.chainId().orElseThrow().value();
        meta.markSnapshotComplete(chain, "pipe-1", "orders");
        String recorded = recordedStart(meta, chain);

        FakeSource direct = new FakeSource(List.of(row(1), row(2)), List.of(change(11)));
        runUnit(direct, meta).start(spec(ReadMode.SNAPSHOT_AND_CDC, false, "chain-flip-off"), e -> { });

        assertThat(direct.cdcStart)
                .as("where the run reads from after the buffer was turned off: the record is the chain's "
                        + "and not the buffer's, so it is still there to be picked up. Kept with the "
                        + "buffer, this is the source's present moment and the changes in between are gone")
                .isEqualTo(CaptureStart.resume(new SourcePosition(recorded)));
    }

    /**
     * And the other way, which is a different question rather than the same one mirrored.
     *
     * <p>A directly-read run writes its position through a path of its own -- it has no buffer to ride
     * along with -- so that the record it leaves is one a buffered run can pick up is a second thing to
     * be true, not a restatement of the first. Both directions are here because an accounting that worked
     * one way and not the other would look correct from whichever side happened to be tested.
     */
    @Test
    void aRunThatTurnsTheBufferOnBeginsWhereTheDirectOneStopped() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource direct = new FakeSource(List.of(row(1), row(2)), List.of(change(20)));
        CaptureRun first = runUnit(direct, meta)
                .start(spec(ReadMode.SNAPSHOT_AND_CDC, false, "chain-flip-on"), e -> { });
        String chain = first.chainId().orElseThrow().value();
        meta.markSnapshotComplete(chain, "pipe-1", "orders");
        String recorded = recordedStart(meta, chain);

        FakeSource buffered = new FakeSource(List.of(row(1), row(2)), List.of(change(21)));
        runUnit(buffered, meta).start(spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-flip-on"), e -> { });

        assertThat(buffered.cdcStart)
                .as("where the run reads from after the buffer was turned back on: what the direct run "
                        + "wrote is on the chain, so the buffered one that follows it begins there")
                .isEqualTo(CaptureStart.resume(new SourcePosition(recorded)));
    }

    /** What the chain says a next run should begin at: the read offset if there is one, else the seam. */
    private static String recordedStart(InMemoryMeta meta, String chain) {
        SrsMeta record = meta.read(chain).orElseThrow();
        String start = record.sourceReadOffset() != null
                ? record.sourceReadOffset()
                : record.consumerOffset("pipe-1").map(ConsumerOffset::cdcStartPosition).orElse(null);
        return Objects.requireNonNull(start, "the first run recorded nothing for the second to pick up");
    }
    /** A run spec for a config-derived chain (no srs.key). */
    private static CaptureRunSpec spec(ReadMode mode, boolean srsEnabled) {
        return spec(mode, srsEnabled, null);
    }

    /**
     * A run spec keyed by an explicit {@code srsKey} so each shared-ring test gets its own mining chain —
     * and so its own per-table ring on the member shared across this class, keeping the tests isolated.
     */
    private static CaptureRunSpec spec(ReadMode mode, boolean srsEnabled, String srsKey) {
        return spec(mode, srsEnabled, srsKey, StartFrom.earliest());
    }

    /** A run spec that pins {@code start_from} rather than taking this fixture's default. */
    private static CaptureRunSpec spec(
            ReadMode mode, boolean srsEnabled, String srsKey, StartFrom startFrom) {
        return new CaptureRunSpec(
                config(), mode, srsKey, srsEnabled, "src-1", "pipe-1", startFrom, null, 0L);
    }

    /** A run spec for a named consumer pipeline on an explicitly keyed chain. */
    private static CaptureRunSpec specFor(String pipelineId, ReadMode mode, String srsKey) {
        return new CaptureRunSpec(
                config(), mode, srsKey, true, "src-1", pipelineId, StartFrom.earliest(), null, 0L);
    }

    private CaptureRunUnit runUnit(CapturePort port, SrsMetaStore meta) {
        return new CaptureRunUnit(port, new SrsCoordinator(meta), meta, hz);
    }

    // ---- a run begun: its load read while the pipeline takes it -------------------------------------

    /**
     * Begun, a run comes back with its load open and not yet read -- the seam recorded, the rows still to
     * come -- and its tail opens only once the load is through. Here the hand-off has no room until the
     * case makes some, which is the state a load of any size meets once the job has fallen behind it: a
     * run that read its load before coming back would not come back at all.
     */
    @Test
    void aRunBegunComesBackBeforeItsLoadIsReadAndOpensItsTailOnlyAfterIt() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1), row(2)), List.of(change(3)));
        CountDownLatch room = new CountDownLatch(1);
        List<String> seen = new CopyOnWriteArrayList<>();
        CaptureHandoff handoff = new CaptureHandoff() {
            @Override
            public void accept(Envelope event) {
                awaitRoom(room);
                seen.add("row:" + event.after().get("id"));
            }

            @Override
            public void loaded(String table) {
                seen.add("loaded:" + table);
            }
        };

        CaptureRun run = runUnit(port, meta).begin(spec(ReadMode.SNAPSHOT_AND_CDC, false, "chain-begun"), handoff);

        assertThat(run.loading()).isTrue();
        assertThat(meta.read(run.chainId().orElseThrow().value()).orElseThrow().consumerOffset("pipe-1"))
                .as("the seam is recorded before the run comes back")
                .get().extracting(ConsumerOffset::cdcStartPosition).isEqualTo("seam-0");
        assertThat(port.cdcStarted).as("no tail while the load is being read").isFalse();
        assertThat(run.cdcSubscription()).isEmpty();

        room.countDown();
        assertThat(run.awaitLoaded(Duration.ofSeconds(10))).isTrue();

        // The tail's change comes through the same hand-off, and behind the load.
        assertThat(seen).containsExactly("row:1", "row:2", "loaded:orders", "row:3");
        assertThat(run.loading()).isFalse();
        assertThat(run.snapshotCount()).isEqualTo(2);
        assertThat(run.snapshotCounts()).containsEntry("orders", 2L);
        assertThat(run.failure()).isEmpty();
        assertThat(run.cdcSubscription()).isPresent();

        run.close();
        assertThat(port.cdcClosed).isTrue();
    }

    @Test
    void aSharedRingRunBegunMinesItsChangesOnlyOnceItsLoadIsThrough() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1)), List.of(change(2)));
        List<String> seen = new CopyOnWriteArrayList<>();
        CaptureHandoff handoff = new CaptureHandoff() {
            @Override
            public void accept(Envelope event) {
                seen.add("row:" + event.after().get("id"));
            }

            @Override
            public void loaded(String table) {
                seen.add("loaded:" + table);
            }
        };

        CaptureRun run = runUnit(port, meta).begin(spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-begun-ring"), handoff);

        assertThat(run.ringSource()).as("the ring's source is there before the load is").isPresent();
        assertThat(run.awaitLoaded(Duration.ofSeconds(10))).isTrue();
        assertThat(seen).containsExactly("row:1", "loaded:orders");
        assertThat(port.cdcStarted).isTrue();
        assertThat(run.failure()).isEmpty();
        run.close();
    }

    /**
     * A stop closes a run whose load is waiting for room that will never come -- the job it was waiting on
     * is gone. It ends there: nothing is reported loaded, no tail opens, and the stop is not a failure.
     */
    @Test
    void closingARunWhoseLoadIsWaitingAbandonsTheLoadAndOpensNoTail() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1), row(2)), List.of(change(3)));
        List<String> loaded = new CopyOnWriteArrayList<>();
        CaptureHandoff handoff = new CaptureHandoff() {
            @Override
            public void accept(Envelope event) {
                awaitRoom(new CountDownLatch(1));
            }

            @Override
            public void loaded(String table) {
                loaded.add(table);
            }
        };
        CaptureRun run = runUnit(port, meta).begin(spec(ReadMode.SNAPSHOT_AND_CDC, false, "chain-abandoned"), handoff);

        run.close();

        assertThat(run.awaitLoaded(Duration.ofSeconds(10))).isTrue();
        assertThat(run.failure()).isEmpty();
        assertThat(loaded).isEmpty();
        assertThat(port.cdcStarted).isFalse();
        assertThat(run.cdcSubscription()).isEmpty();
    }

    @Test
    void aLoadThatFailsWhileItIsReadIsTheRunsFailure() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1), row(2)), List.of(change(3)));
        IllegalStateException refused = new IllegalStateException("the hand-off refused a row");
        CaptureHandoff handoff = new CaptureHandoff() {
            @Override
            public void accept(Envelope event) {
                throw refused;
            }

            @Override
            public void loaded(String table) {
                throw new AssertionError("a table whose read failed is not loaded");
            }
        };

        CaptureRun run = runUnit(port, meta).begin(spec(ReadMode.SNAPSHOT_AND_CDC, false, "chain-failed"), handoff);

        assertThat(run.awaitLoaded(Duration.ofSeconds(10))).isTrue();
        assertThat(run.failure()).containsSame(refused);
        assertThat(port.cdcStarted).isFalse();
    }

    /**
     * A load cut short by something other than the run being closed is a failure, whatever shape it takes.
     * The hand-off throws the same exception a closed run's load ends with when it is released under a load
     * nobody abandoned, and a connector can throw one of its own. Taken for an abandonment, the load ends
     * with nothing reported, no tail opened and nothing failed, and a pipeline waiting on that load waits for
     * good while it reads as healthy.
     */
    @Test
    void aLoadCutShortByACancellationTheRunDidNotAskForIsTheRunsFailure() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1), row(2)), List.of(change(3)));
        CancellationException released =
                new CancellationException("the pipeline's hand-off was released while its load ran");
        List<String> loaded = new CopyOnWriteArrayList<>();
        CaptureHandoff handoff = new CaptureHandoff() {
            @Override
            public void accept(Envelope event) {
                throw released;
            }

            @Override
            public void loaded(String table) {
                loaded.add(table);
            }
        };

        CaptureRun run = runUnit(port, meta).begin(spec(ReadMode.SNAPSHOT_AND_CDC, false, "chain-cut"), handoff);

        assertThat(run.awaitLoaded(Duration.ofSeconds(10))).isTrue();
        assertThat(run.failure()).containsSame(released);
        assertThat(loaded).isEmpty();
        assertThat(port.cdcStarted).isFalse();
    }

    /**
     * A run whose load is abandoned while other pipelines still read the capture it tails goes on to open
     * that tail. The load was one pipeline's, and that pipeline has stopped, so nothing of it is reported and
     * nothing about it fails the run. The tail is everybody's, and it begins at the load's own seam, as it
     * would have after the last row.
     */
    @Test
    void aRunWhoseLoadIsAbandonedGoesOnToOpenItsTail() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        List<String> tail = new CopyOnWriteArrayList<>();
        LoadThenTailSource port = new LoadThenTailSource(List.of(row(1), row(2)), () -> {
            // A source that waits on its own start refuses on a thread already interrupted, as a real one does:
            // whatever woke the read must not reach the tail.
            if (Thread.currentThread().isInterrupted()) {
                throw new IllegalStateException("the tail was opened on an interrupted thread");
            }
            tail.add("opened");
            return () -> tail.add("closed");
        });
        CountDownLatch released = new CountDownLatch(1);
        List<String> loaded = new CopyOnWriteArrayList<>();
        CaptureHandoff handoff = new CaptureHandoff() {
            @Override
            public void accept(Envelope event) {
                // Waits for room nobody makes until it is woken, or the hand-off is released under it, and ends
                // the way the real one does either way.
                awaitRoom(released);
                throw new CancellationException("the pipeline's hand-off was released while its load ran");
            }

            @Override
            public void loaded(String table) {
                loaded.add(table);
            }
        };
        CaptureRun run = runUnit(port, meta).begin(spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-left"), handoff);

        run.abandonLoad();
        released.countDown();

        assertThat(run.awaitLoaded(Duration.ofSeconds(10))).isTrue();
        assertThat(run.failure()).isEmpty();
        assertThat(loaded).isEmpty();
        assertThat(tail).as("the tail the others read opens all the same").containsExactly("opened");
        assertThat(port.cdcStart).isEqualTo(CaptureStart.resume(new SourcePosition("seam-0")));
        assertThat(run.cdcSubscription()).isPresent();
        run.close();
        assertThat(tail).containsExactly("opened", "closed");
    }

    @Test
    void aTailThatCannotOpenAfterTheLoadIsTheRunsFailure() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        IllegalStateException refused = new IllegalStateException("the source refused the tail");
        LoadThenTailSource port = new LoadThenTailSource(List.of(row(1)), () -> {
            throw refused;
        });
        List<String> loaded = new CopyOnWriteArrayList<>();

        CaptureRun run = runUnit(port, meta).begin(
                spec(ReadMode.SNAPSHOT_AND_CDC, false, "chain-tailless"), handoffInto(new ArrayList<>(), loaded));

        assertThat(run.awaitLoaded(Duration.ofSeconds(10))).isTrue();
        assertThat(loaded).containsExactly("orders");
        assertThat(run.failure()).containsSame(refused);
        assertThat(run.cdcSubscription()).isEmpty();
    }

    /** Closed by something the tail's opening calls, the run closes the tail that opening hands back. */
    @Test
    void aRunClosedWhileItsTailOpensClosesThatTail() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        AtomicReference<CaptureRun> begun = new AtomicReference<>();
        List<String> closed = new CopyOnWriteArrayList<>();
        CountDownLatch handedBack = new CountDownLatch(1);
        LoadThenTailSource port = new LoadThenTailSource(List.of(row(1)), () -> {
            awaitRoom(handedBack);
            begun.get().close();
            return () -> closed.add("tail");
        });

        begun.set(runUnit(port, meta).begin(
                spec(ReadMode.SNAPSHOT_AND_CDC, false, "chain-closing"), handoffInto(new ArrayList<>(), new ArrayList<>())));
        handedBack.countDown();

        assertThat(begun.get().awaitLoaded(Duration.ofSeconds(10))).isTrue();
        assertThat(closed).containsExactly("tail");
        assertThat(begun.get().cdcSubscription()).isEmpty();
        assertThat(begun.get().failure()).isEmpty();
    }

    /** With no load owed there is nothing to read, and a run begun is started whole, tail included. */
    @Test
    void aRunOwingNoLoadIsStartedWholeWhenBegun() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1)), List.of(change(2)));

        CaptureRun run = runUnit(port, meta).begin(
                spec(ReadMode.CDC_ONLY, false, "chain-cdc-begun"), handoffInto(new ArrayList<>(), new ArrayList<>()));

        assertThat(run.loading()).isFalse();
        assertThat(port.cdcStarted).isTrue();
        assertThat(run.cdcSubscription()).isPresent();
        run.close();
    }

    @Test
    void aChainlessLoadBegunReportsItsTablesOnceItsReadIsThrough() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1), row(2), row(3)), List.of());
        List<Envelope> rows = new CopyOnWriteArrayList<>();
        List<String> loaded = new CopyOnWriteArrayList<>();

        CaptureRun run = runUnit(port, meta).begin(spec(ReadMode.SNAPSHOT_ONLY, true), handoffInto(rows, loaded));

        assertThat(run.awaitLoaded(Duration.ofSeconds(10))).isTrue();
        assertThat(rows).extracting(e -> e.after().get("id")).containsExactly(1, 2, 3);
        assertThat(loaded).containsExactly("orders");
        assertThat(run.chainId()).isEmpty();
        assertThat(run.cdcSubscription()).isEmpty();
        assertThat(port.cdcStarted).isFalse();
    }

    /** A hand-off that takes every row into {@code rows} and every loaded table into {@code loaded}. */
    private static CaptureHandoff handoffInto(List<Envelope> rows, List<String> loaded) {
        return new CaptureHandoff() {
            @Override
            public void accept(Envelope event) {
                rows.add(event);
            }

            @Override
            public void loaded(String table) {
                loaded.add(table);
            }
        };
    }

    /** Waits for {@code room}; an interrupt ends the wait the way the real hand-off's wait ends. */
    private static void awaitRoom(CountDownLatch room) {
        try {
            if (!room.await(30, TimeUnit.SECONDS)) {
                throw new AssertionError("no room was ever made");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new CancellationException("interrupted while waiting for room");
        }
    }

    @Test
    void snapshotOnlyDrainsToThePassthroughWithNoChainNoCdcStartAndNoTail() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1), row(2), row(3)), List.of());
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta).start(spec(ReadMode.SNAPSHOT_ONLY, true), passthrough::add);

        // snapshot_only is a bounded pass straight to the sink: no shared chain a cdc tail resumes against,
        // so nothing is provisioned, no cdc-start is recorded, and no tail is attached.
        assertThat(passthrough).extracting(e -> e.after().get("id")).containsExactly(1, 2, 3);
        assertThat(passthrough).extracting(event -> event.position().order())
                .containsOnly(SourceOrder.snapshotRow(1L));
        assertThat(run.snapshotCount()).isEqualTo(3);
        assertThat(run.snapshotCounts()).containsEntry("orders", 3L);
        assertThat(run.chainId()).isEmpty();
        assertThat(run.ringSource()).isEmpty();
        assertThat(run.cdcSubscription()).isEmpty();
        assertThat(meta.created).isEmpty();
        assertThat(port.cdcStarted).isFalse();
    }

    @Test
    void directSnapshotOnlyRunsWithoutAnAssignedGenerationStillAdvanceLocally() {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureRunUnit unit = runUnit(new FakeSource(List.of(row(1)), List.of()), meta);
        List<SourceOrder> accepted = new ArrayList<>();

        unit.start(spec(ReadMode.SNAPSHOT_ONLY, true), event -> accepted.add(event.position().order()));
        unit.start(spec(ReadMode.SNAPSHOT_ONLY, true), event -> accepted.add(event.position().order()));

        assertThat(accepted).containsExactly(
                SourceOrder.snapshotRow(1L), SourceOrder.snapshotRow(2L));
    }

    @Test
    @DisplayName("what the load read is on the run's own account, under the operation the source performed")
    void theLoadsRowsAreCountedOnTheRunsAccountAsReads() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1), row(2), row(3)), List.of());

        CaptureRun run = runUnit(port, meta).start(spec(ReadMode.SNAPSHOT_ONLY, true), event -> { });

        // The account is opened before the load, not with the tail that follows it. Opened after, a run
        // would report having read nothing until its first change arrived - and for a snapshot_only run,
        // which never opens a tail at all, for ever.
        assertThat(run.health().receivedRows()).containsExactly(Map.entry("orders", Map.of("r", 3L)));
    }

    /**
     * A second run over a chain that has already been read picks up where the first left off: it does not
     * re-read the full load, and its tail begins at the recorded position rather than at the source's
     * present moment.
     *
     * <p>Both halves are the same failure seen from two sides. Starting the tail at the present drops
     * every change made since the last run stopped; re-reading the full load re-sends rows the sink has
     * already taken. The first is silent and the second is merely slow, which is why only the first has
     * ever been noticed.
     *
     * <p>The two runs share a meta store and get separate coordinators, which is what a restart is: the
     * durable record survives, the in-memory chain state does not.
     *
     * <p>What makes a table done is the sink confirming it, and these runs have no sink: the confirmation
     * is stood in for here. Reading a table is not writing it, so the read side records nothing -- a run
     * that skipped a table on the strength of having read it would drop every row of it that has not
     * changed since.
     */
    @Test
    void aSecondRunResumesFromTheRecordedPositionInsteadOfReReadingFromThePresent() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource first = new FakeSource(List.of(row(1), row(2)), List.of(change(10)));
        CaptureRun firstRun =
                runUnit(first, meta).start(spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-resume"), e -> { });
        meta.markSnapshotComplete(firstRun.chainId().orElseThrow().value(), "pipe-1", "orders");

        FakeSource restarted = new FakeSource(List.of(row(1), row(2)), List.of(change(11)));
        CaptureRun second = runUnit(restarted, meta)
                .start(spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-resume"), e -> { });

        assertThat(second.snapshotCount())
                .as("the full load already finished for every selected table, so it is not read again")
                .isZero();
        assertThat(restarted.cdcStart)
                .as("the tail resumes at the recorded seam rather than at the source's present moment")
                .isEqualTo(CaptureStart.resume(new SourcePosition("seam-0")));
    }

    /**
     * The case above with its one stand-in removed: nothing confirms the write, so the table is read again.
     *
     * <p>These are the two halves of one rule, and only together do they discriminate. A table is owed
     * until a sink has confirmed it, which is a different question from whether it was read -- and only the
     * confirmed one is safe to skip on. A run that took the read for the answer would skip this table on
     * the way back, and every row of it that has not changed since would be absent from the target for
     * good: the tail only replays what changed after the seam, so nothing would ever fetch them again.
     * Nothing thrown, nothing logged.
     *
     * <p>Asserted on the assembled run rather than on the snapshot phase alone, which is the whole of why
     * it is here. The phase's own tests cover the phase; they stay green if the mark is made by whatever
     * calls it, once the read returns. That is a read-side mark by another name, and this is the reading
     * that sees it.
     */
    @Test
    void aTableNoSinkConfirmedIsReadAgainByTheRunThatFollows() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource first = new FakeSource(List.of(row(1), row(2)), List.of(change(10)));
        CaptureRun firstRun =
                runUnit(first, meta).start(spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-unacked"), e -> { });
        String chainId = firstRun.chainId().orElseThrow().value();

        // The read drained every row of the table, and no sink confirmed any of them.
        assertThat(firstRun.snapshotCount()).isEqualTo(2);
        assertThat(meta.read(chainId)).get()
                .extracting(record -> record.snapshotCompletedTables("pipe-1"))
                .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.list(String.class))
                .isEmpty();

        FakeSource restarted = new FakeSource(List.of(row(1), row(2)), List.of(change(11)));
        CaptureRun second = runUnit(restarted, meta)
                .start(spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-unacked"), e -> { });

        assertThat(second.snapshotCount())
                .as("no sink confirmed the table, so the full load is owed and read again")
                .isEqualTo(2);
    }

    /**
     * A pipeline new to a chain reads its own full load, whatever the pipelines already on that chain
     * have finished.
     *
     * <p>Completion answers "are this pipeline's rows in this pipeline's target", and every pipeline on a
     * chain has a target of its own. A chain is keyed by the source connection alone -- the table subset
     * is deliberately not part of it -- so two pipelines reading one database share a chain by
     * construction, and a mark left by the first answers the second's question with the first's answer.
     *
     * <p>What a chain shares is the mining: the source's change log is read once for everyone on it. The
     * initial load is not part of that. A new pipeline's target starts empty, so its rows can only come
     * from a read of its own, and a second read of the source is what that costs.
     *
     * <p>Asserted on the per-table counts rather than the total, because the two failures differ: a run
     * that never entered the snapshot phase reports an empty map, and a run that read an empty table
     * reports a zero. Only the first is this defect, and a total of zero cannot tell them apart.
     */
    @Test
    void aPipelineNewToAChainReadsItsOwnFullLoad() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource first = new FakeSource(List.of(row(1), row(2)), List.of(change(10)));
        CaptureRun firstRun = runUnit(first, meta)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-shared"), e -> { });
        String chainId = firstRun.chainId().orElseThrow().value();
        // Stands in for pipe-a's sink confirming the table -- the only thing that ever marks one done.
        meta.markSnapshotComplete(chainId, "pipe-a", "orders");

        FakeSource second = new FakeSource(List.of(row(1), row(2)), List.of(change(11)));
        CaptureRun secondRun = runUnit(second, meta)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-shared"), e -> { });

        assertThat(secondRun.snapshotCounts())
                .as("pipe-b's target is empty, so it owes itself every row of the table pipe-a finished")
                .containsExactlyInAnyOrderEntriesOf(Map.of("orders", 2L));
    }

    /**
     * A pipeline new to a chain begins its tail at the seam its own load sampled, not at the one the
     * chain was created at.
     *
     * <p>The same reckoning as the load above, one field over. A recorded seam says where the snapshot
     * that recorded it began, and that snapshot belongs to one pipeline. A pipeline new to the chain has
     * a seam of its own, sampled by its own bounded read moments ago, while the recorded one may be days
     * old -- and a source keeps its change log for a window and refuses a start from before it. Handed
     * the chain's, every join to a chain older than that window fails; and where a source answers such a
     * start with an empty stream instead of a refusal, the pipeline comes up healthy, reports running and
     * delivers nothing, which is the same shape as a source that has no changes for it.
     *
     * <p>Reaching back to the chain's birth buys a joiner nothing either. What a chain shares is the
     * mining; the initial load is not part of that, so the joiner reads the source in full for itself and
     * every change before its own seam is already covered by that read.
     */
    @Test
    void aPipelineNewToAChainBeginsItsTailAtItsOwnSeamNotTheOneTheChainWasCreatedAt() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource first = new FakeSource(List.of(row(1), row(2)), List.of(), "seam-the-chain-began-at");
        CaptureRun firstRun = runUnit(first, meta)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-joined"), e -> { });
        String chainId = firstRun.chainId().orElseThrow().value();
        // Stands in for pipe-a's sink confirming the table -- the only thing that ever marks one done.
        meta.markSnapshotComplete(chainId, "pipe-a", "orders");

        FakeSource joiner = new FakeSource(List.of(row(1), row(2)), List.of(), "seam-the-joiner-began-at");
        runUnit(joiner, meta)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-joined"), e -> { });

        assertThat(joiner.cdcStart)
                .as("the joiner's tail begins where its own load began, not where the chain did")
                .isEqualTo(CaptureStart.resume(new SourcePosition("seam-the-joiner-began-at")));
    }

    @Test
    void aPipelineWhoseCompletedLoadSamplesNoSeamRestartsAtItsOwnSeam() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource first = new FakeSource(List.of(row(1), row(2)), List.of(), "seam-chain-birth");
        CaptureRun firstRun = runUnit(first, meta)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-completed-join"), e -> { });
        String chainId = firstRun.chainId().orElseThrow().value();
        meta.markSnapshotComplete(chainId, "pipe-a", "orders");

        FakeSource joiner = new FakeSource(List.of(row(1), row(2)), List.of(), "seam-join");
        runUnit(joiner, meta)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-completed-join"), e -> { });
        meta.markSnapshotComplete(chainId, "pipe-b", "orders");

        FakeSource restarted = new FakeSource(List.of(row(1), row(2)), List.of(), "seam-not-sampled");
        runUnit(restarted, meta)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-completed-join"), e -> { });

        assertThat(restarted.cdcStart)
                .as("pipe-b owes no table on restart, so its tail must not adopt pipe-a's older seam")
                .isEqualTo(CaptureStart.resume(new SourcePosition("seam-join")));
    }

    @Test
    void aCdcOnlyPipelineDoesNotAdoptAnotherPipelinesSnapshotSeam() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource loader = new FakeSource(List.of(row(1)), List.of(), "seam-loader");
        runUnit(loader, meta)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-cdc-only-join"), e -> { });

        FakeSource cdcOnly = new FakeSource(List.of(), List.of(), "seam-never-sampled");
        CaptureRun run = runUnit(cdcOnly, meta)
                .start(specFor("pipe-b", ReadMode.CDC_ONLY, "chain-cdc-only-join"), e -> { });

        assertThat(run.snapshotCount()).as("cdc_only has no load from which to sample a seam").isZero();
        assertThat(cdcOnly.cdcStart)
                .as("with no shared read or seam of its own, pipe-b uses the start for this run")
                .isEqualTo(CaptureStart.present());
    }

    /**
     * A pipeline told to re-read everything does, on a chain another pipeline is still using -- and the
     * other one is not made to re-read anything.
     *
     * <p>Giving back this pipeline's own record is the whole of what a clearing stop does while somebody
     * else is on the chain, and the tables it had finished are part of that record. They used to be the
     * chain's, so there was nowhere to clear them from without deciding it on every other consumer's
     * behalf -- they were left alone, and the next run skipped the load the operator had just asked for.
     * The command reported success and the target kept whatever it had.
     *
     * <p>Both halves are asserted because each fails on its own, and each failure is silent. A rerun that
     * still reads nothing is the defect this closes; a rerun that made its neighbour re-read its whole
     * source is the one the chain-level record was avoiding.
     */
    @Test
    void aPipelineToldToRereadEverythingDoesSoWithoutDisturbingItsChainNeighbour() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1), row(2)), List.of(change(10)));
        CaptureRun aRun = runUnit(port, meta)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-rerun"), e -> { });
        String chainId = aRun.chainId().orElseThrow().value();
        runUnit(new FakeSource(List.of(row(1), row(2)), List.of(change(11))), meta)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-rerun"), e -> { });
        // Both sinks confirmed the table, which is what makes a plain restart read nothing.
        meta.markSnapshotComplete(chainId, "pipe-a", "orders");
        meta.markSnapshotComplete(chainId, "pipe-b", "orders");

        // What a clearing stop leaves behind on a chain somebody else is still reading: this pipeline's
        // own record, gone; everything shared, untouched.
        meta.detachConsumer(chainId, "pipe-a");

        CaptureRun reran = runUnit(new FakeSource(List.of(row(1), row(2)), List.of(change(12))), meta)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-rerun"), e -> { });
        assertThat(reran.snapshotCounts())
                .as("the pipeline that asked to re-read everything reads its whole table again")
                .containsExactlyInAnyOrderEntriesOf(Map.of("orders", 2L));

        CaptureRun neighbour = runUnit(new FakeSource(List.of(row(1), row(2)), List.of(change(13))), meta)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-rerun"), e -> { });
        assertThat(neighbour.snapshotCounts())
                .as("and the pipeline that asked for nothing still owes nothing")
                .isEmpty();
    }

    @Test
    void snapshotAndCdcOverASharedRingProvisionsSnapshotsAttachesAndWritesTheChangeRing() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1), row(2)), List.of(change(10), change(11)));
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta).start(spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-snap-cdc"), passthrough::add);

        // The full source run: the chain is provisioned and seeded, the snapshot drains straight to the sink
        // (recording the cdc-start position at the seam), the consumer attaches, and the cdc tail writes the
        // shared change ring the exposed Jet source reads.
        assertThat(run.chainId()).isPresent();
        assertThat(run.merged()).isFalse();
        assertThat(run.snapshotCount()).isEqualTo(2);
        assertThat(passthrough).extracting(e -> e.after().get("id")).containsExactly(1, 2);
        assertThat(run.ringSource()).isPresent();
        assertThat(run.cdcSubscription()).isPresent();
        assertThat(port.cdcStarted).isTrue();

        String chainId = run.chainId().get().value();
        assertThat(meta.created).containsExactly(chainId);
        // The seam the source itself sampled, not a constant this layer supplied: the recorded value is
        // the batch's own, which is what makes the tail's join to the snapshot a real one.
        assertThat(meta.read(chainId).orElseThrow().consumerOffset("pipe-1")).get()
                .extracting(ConsumerOffset::cdcStartPosition)
                .isEqualTo("seam-0");

        Ringbuffer<SrsItem> ring = hz.getRingbuffer(SrsRingbuffer.ringName(chainId, "orders"));
        assertThat(ring.tailSequence()).isEqualTo(1L);
        assertThat(ring.readOne(0).after()).containsEntry("id", 10);
        assertThat(ring.readOne(1).after()).containsEntry("id", 11);
    }

    @Test
    void cdcOnlyOverASharedRingSkipsTheSnapshotButStillProvisionsAndWritesTheRing() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1)), List.of(change(10), change(11)));
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta).start(spec(ReadMode.CDC_ONLY, true, "chain-cdc-only"), passthrough::add);

        // cdc_only skips the initial snapshot: nothing drains to the sink and no cdc-start position is
        // recorded (there is no snapshot seam), but the chain is still provisioned and the tail writes the ring.
        assertThat(run.snapshotCount()).isEqualTo(0);
        assertThat(passthrough).isEmpty();
        assertThat(run.chainId()).isPresent();
        assertThat(run.ringSource()).isPresent();
        assertThat(run.cdcSubscription()).isPresent();

        String chainId = run.chainId().get().value();
        ConsumerOffset offset = meta.read(chainId).orElseThrow().consumerOffset("pipe-1").orElseThrow();
        assertThat(offset.perTableSeq()).containsEntry("orders", -1L);
        assertThat(offset.cdcStartPosition()).isNull();
        Ringbuffer<SrsItem> ring = hz.getRingbuffer(SrsRingbuffer.ringName(chainId, "orders"));
        assertThat(ring.tailSequence()).isEqualTo(1L);
    }

    @Test
    void srsDisabledStreamsTheTailStraightToThePassthroughWithNoRingButKeepsTheRecord() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(), List.of(change(10), change(11)));
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta).start(spec(ReadMode.CDC_ONLY, false), passthrough::add);

        // srs.enabled:false is the direct path: the cdc tail streams straight to the single consumer with
        // no shared ring. What the flag does not turn off is the account -- the chain is opened and its
        // durable record seeded, because that record is what the run after this one starts from.
        assertThat(passthrough).extracting(e -> e.after().get("id")).containsExactly(10, 11);
        assertThat(run.ringSource()).isEmpty();
        assertThat(run.cdcSubscription()).isPresent();
        assertThat(port.cdcStarted).isTrue();
        assertThat(run.chainId()).isPresent();
        assertThat(meta.created).containsExactly(run.chainId().orElseThrow().value());
    }

    /** A direct tail resumes from its own channel's durable recovery record rather than the present. */
    @Test
    void aDirectTailBeginsWhereTheRecordSaysRatherThanAtThePresent() {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureRunSpec spec = spec(ReadMode.CDC_ONLY, false, "chain-direct-resume");
        MiningChainId chainId = spec.miningChainId();
        meta.create(chainId.value(), null);
        meta.advanceSourceReadOffset(chainId.value(), new ChainPosition(new SourceOrder(1L, 7L), "src-11"));

        FakeSource port = new FakeSource(List.of(), List.of(change(12)));
        CaptureRun run = runUnit(port, meta)
                .start(spec, e -> { });

        assertThat(port.cdcStart)
                .as("the direct tail picks up at the recorded position, not at the source's present moment")
                .isEqualTo(CaptureStart.resume(new SourcePosition("src-11")));
        assertThat(run.chainId())
                .as("the independent channel retains its own recovery record")
                .contains(chainId);
        assertThat(run.ringSource())
                .as("no ring: that half of it does follow the flag")
                .isEmpty();
    }

    /**
     * A direct tail writes down how far the source has been safely processed in its own channel's account.
     * Without that account the next run has no recovery position and loses changes in between.
     *
     * <p>The offset only ever moves to a position a consumer has durably landed. Reading is not writing,
     * and an offset that ran ahead of the sink would skip, on the way back, changes no sink ever took. A
     * sink confirmation is therefore stood in for here, high enough that the clamp is not what this case
     * measures; the case below measures the clamp itself.
     */
    @Test
    void aDirectTailRecordsHowFarTheSourceHasBeenReadOnceASinkHasLandedIt() {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureRunSpec spec = spec(ReadMode.CDC_ONLY, false, "chain-direct-offset");
        MiningChainId chainId = spec.miningChainId();
        meta.create(chainId.value(), null);
        meta.advanceSinkAcked(chainId.value(), "pipe-1",
                new ChainPosition(new SourceOrder(Long.MAX_VALUE, Long.MAX_VALUE), "landed"));

        FakeSource port = new FakeSource(List.of(), List.of(change(10), change(11)));
        runUnit(port, meta).start(spec, e -> { });

        assertThat(meta.read(chainId.value()).orElseThrow().sourceReadOffset())
                .as("the direct tail retains its own safely processed recovery position")
                .isEqualTo("src-11");
    }

    /**
     * The case above with its stand-in removed: no consumer has landed anything, so nothing is written down.
     *
     * <p>The two are one rule seen from both sides, and only together do they discriminate. An offset is a
     * claim that everything below it is safely out of the source's reach -- true only once a sink has taken
     * it, because the direct tail buffers nothing and a change it forwarded but nobody wrote is gone the
     * moment the process is. An implementation that recorded the read unconditionally passes the case above
     * and fails here, which is the only place that difference is visible.
     */
    @Test
    void aDirectTailRecordsNothingWhileNoSinkHasLandedAnything() {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureRunSpec spec = spec(ReadMode.CDC_ONLY, false, "chain-direct-unacked");
        MiningChainId chainId = spec.miningChainId();

        FakeSource port = new FakeSource(List.of(), List.of(change(10), change(11)));
        runUnit(port, meta).start(spec, e -> { });

        assertThat(meta.read(chainId.value()).orElseThrow().sourceReadOffset())
                .as("read is not written: an offset ahead of the sink would skip changes on the way back")
                .isNull();
    }

    /**
     * A direct tail stamps each change with an order, so a sink downstream can rank and ack it.
     *
     * <p>A direct tail has no ring, and the ring's sequence is where a buffered change's order comes from.
     * Leaving the order off is not the neutral choice it looks like: every downstream that ranks positions
     * drops one that carries none, so an unstamped direct tail is one nothing can ever confirm, and an
     * account nothing confirms never advances. The count of changes this run has forwarded is the sequence
     * instead -- monotonic within the generation, exactly like the ring's, and taken afresh with each new
     * generation the chain opens.
     */
    @Test
    void aDirectTailStampsEachChangeWithAnOrderSoASinkCanRankIt() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(), List.of(change(10), change(11)));
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta)
                .start(spec(ReadMode.CDC_ONLY, false, "chain-direct-order"), passthrough::add);

        long epoch = meta.read(run.chainId().orElseThrow().value()).orElseThrow().epoch();
        assertThat(passthrough).extracting(e -> e.position().order())
                .as("each change is ordered within the generation the chain opened for this run")
                .containsExactly(new SourceOrder(epoch, 0L), new SourceOrder(epoch, 1L));
        assertThat(passthrough).extracting(e -> e.position().token())
                .as("the token the source named for a run rides with the change that closes it")
                .containsExactly("src-10", "src-11");
    }

    @Test
    void srsDisabledSurfacesADeadTailAsAFailureOnTheRun() {
        InMemoryMeta meta = new InMemoryMeta();
        RuntimeException boom = new RuntimeException("tail boom");
        FakeSource port = new FakeSource(List.of(), List.of()).failing(boom);
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta).start(spec(ReadMode.CDC_ONLY, false), passthrough::add);

        // The direct tail reported a failure; the run surfaces it so a coordinator polling the run can see a
        // dead tail rather than a run that merely stopped emitting.
        assertThat(run.failure()).contains(boom);
    }

    @Test
    void surfacesTheForceMergeWhenASecondSourceResolvesToTheSameChain() {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureRunUnit unit = new CaptureRunUnit(
                new FakeSource(List.of(), List.of()), new SrsCoordinator(meta), meta, hz);

        CaptureRun first = unit.start(spec(ReadMode.CDC_ONLY, true, "chain-merge"), e -> { });
        CaptureRun second = unit.start(spec(ReadMode.CDC_ONLY, true, "chain-merge"), e -> { });

        // The first source opens the chain; a second source resolving to the same chain force-merges onto it
        // rather than mining the source twice -- the signal a caller surfaces as a shared capture.
        assertThat(first.merged()).isFalse();
        assertThat(second.merged()).isTrue();
    }

    @Test
    void anAttachedPipelineDoesNotOpenASecondTailForTheCaptureOwner() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(), List.of(change(10)));
        CaptureRunUnit unit = new CaptureRunUnit(port, new SrsCoordinator(meta), meta, hz);

        CaptureRun owner = unit.start(specFor("pipe-a", ReadMode.CDC_ONLY, "chain-owned"), e -> { }, true);
        CaptureRun attached = unit.start(specFor("pipe-b", ReadMode.CDC_ONLY, "chain-owned"), e -> { }, false);

        assertThat(port.cdcStarts).isEqualTo(1);
        assertThat(owner.cdcSubscription()).isPresent();
        assertThat(attached.cdcSubscription()).isEmpty();
        assertThat(attached.chainId()).isEqualTo(owner.chainId());
    }

    /**
     * A pipeline attaching on a member that does not hold the capture reads under the generation the
     * member holding it opened.
     *
     * <p>The member holding a capture opens the chain's ring generation as it starts the tail, and every
     * change that tail writes is ordered under it. A pipeline driven by another member attaches to the same
     * ring with no tail of its own, and the rows of its own load are ordered by the generation stamped on
     * them: beneath every change of that generation, above every change of an older one. An attaching
     * member that opened a generation of its own would put its load above the changes the holder goes on
     * writing, so a row the source changed after the load read it would keep the value the load saw; and
     * every run assembled afterwards would read a generation no tail writes under.
     */
    @Test
    void aPipelineAttachingOnAMemberThatDidNotOpenTheChainReadsUnderTheGenerationAlreadyRunning() {
        InMemoryMeta meta = new InMemoryMeta();
        String chain = MiningChainId.resolve(config(), "chain-held-elsewhere").value();
        CaptureRun held = new CaptureRunUnit(
                new FakeSource(List.of(row(1)), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-held-elsewhere"), e -> { }, true);
        long running = meta.read(chain).orElseThrow().epoch();

        // Another member: a coordinator of its own, over the same durable record and the same ring.
        List<Envelope> loaded = new ArrayList<>();
        CaptureRun attached = new CaptureRunUnit(
                new FakeSource(List.of(row(1), row(2)), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-held-elsewhere"), loaded::add, false);

        assertThat(meta.read(chain).orElseThrow().epoch())
                .as("attaching opened no generation of its own")
                .isEqualTo(running);
        assertThat(loaded)
                .as("it ran a load of its own")
                .hasSize(2);
        assertThat(loaded).extracting(e -> e.position().order())
                .as("and that load is ordered beneath every change of the generation the holder writes under")
                .containsOnly(SourceOrder.snapshotRow(running));
        assertThat(attached.cdcSubscription()).as("it opened no tail").isEmpty();
        assertThat(held.cdcSubscription()).as("the holder's tail is the one tail").isPresent();
    }

    /**
     * Where a pipeline arriving on a chain starts reading the ring is marked as it arrives, before its own
     * load reads anything: just past what the ring already holds. Everything under the mark is history from
     * before the pipeline existed, or a change its own load covers; left unmarked, the pipeline's run reads
     * the ring from its head and hands its target every change the ring has ever kept.
     */
    @Test
    void aPipelineArrivingOnARingThatAlreadyHoldsChangesStartsPastThem() {
        InMemoryMeta meta = new InMemoryMeta();
        String chain = MiningChainId.resolve(config(), "chain-arrival").value();
        new CaptureRunUnit(new FakeSource(List.of(row(1)), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-arrival"), e -> { }, true);
        SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer(SrsRingbuffer.ringName(chain, "orders")));
        ring.append(buffered(1));
        long heldAtArrival = ring.append(buffered(2));

        new CaptureRunUnit(new FakeSource(List.of(row(1)), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-arrival"), e -> { }, false);

        assertThat(meta.ringDoneThrough(chain, "pipe-b"))
                .as("the pipeline starts just past the two changes the ring held when it arrived")
                .containsExactly(Map.entry("orders", heldAtArrival));
    }

    @Test
    void tableRegistrationCompletesBeforeArrivalCanBePublished() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        String chainId = MiningChainId.resolve(config(), "chain-arrival-registration").value();
        new CaptureRunUnit(new FakeSource(List.of(row(1)), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(specFor("pipe-owner", ReadMode.SNAPSHOT_AND_CDC, "chain-arrival-registration"),
                        event -> { }, true);
        // The established reader is keeping up, so only the attaching reader can constrain this write.
        meta.advanceConsumerReadSeq(chainId, "pipe-owner", "orders", 100L);

        CountDownLatch registrationReached = new CountDownLatch(1);
        CountDownLatch allowRegistration = new CountDownLatch(1);
        meta.pauseRegistration("pipe-joining", registrationReached, allowRegistration);
        AtomicReference<Throwable> joiningFailure = new AtomicReference<>();
        Thread joining = new Thread(() -> {
            try {
                new CaptureRunUnit(
                        new FakeSource(List.of(row(2)), List.of()), new SrsCoordinator(meta), meta, hz)
                        .start(specFor("pipe-joining", ReadMode.SNAPSHOT_AND_CDC,
                                "chain-arrival-registration"), event -> { }, false);
            } catch (Throwable failure) {
                joiningFailure.set(failure);
            }
        }, "joining-consumer");
        joining.setDaemon(true);

        String ringName = SrsRingbuffer.ringName(chainId, "orders");
        SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer(ringName));
        AtomicReference<Throwable> beforeArrivalFailure = new AtomicReference<>();
        AtomicReference<Throwable> afterArrivalFailure = new AtomicReference<>();
        Thread afterArrival = null;
        try {
            joining.start();
            assertThat(registrationReached.await(2, TimeUnit.SECONDS))
                    .as("table registration is paused before it becomes visible")
                    .isTrue();
            assertThat(meta.ringDoneThrough(chainId, "pipe-joining"))
                    .as("arrival cannot be published before table registration")
                    .isEmpty();

            List<Envelope> beforeArrivalChanges = new ArrayList<>();
            for (int id = 0; id < 9; id++) {
                beforeArrivalChanges.add(change(id));
            }
            CdcChain chain = new CdcChain(
                    new SrsWriteGate(ring), meta, chainId,
                    meta.read(chainId).orElseThrow().epoch(), 0L);
            Thread beforeArrival = new Thread(() -> {
                try {
                    CdcPhase.run(new FakeSource(List.of(), beforeArrivalChanges), config(), chain,
                            () -> meta.consumerOffsets(chainId), new CaptureHealth());
                } catch (Throwable failure) {
                    beforeArrivalFailure.set(failure);
                }
            }, "writer-before-arrival");
            beforeArrival.start();
            beforeArrival.join(2000);
            assertThat(beforeArrival.isAlive()).isFalse();
            assertThat(beforeArrivalFailure.get()).isNull();
            assertThat(ring.tailSequence()).isEqualTo(8L);
            assertThat(ring.headSequence()).isEqualTo(1L);

            allowRegistration.countDown();
            joining.join(2000);
            assertThat(joining.isAlive()).isFalse();
            assertThat(joiningFailure.get()).isNull();
            assertThat(meta.ringDoneThrough(chainId, "pipe-joining"))
                    .as("changes written before registration landed precede the eventual arrival")
                    .containsExactly(Map.entry("orders", 8L));
            assertThat(meta.read(chainId).orElseThrow()
                    .consumerOffset("pipe-joining").orElseThrow().perTableSeq())
                    .as("arrival lifts the conservative cursor to the same sequence")
                    .containsEntry("orders", 8L);

            List<Envelope> afterArrivalChanges = new ArrayList<>();
            for (int id = 9; id < 18; id++) {
                afterArrivalChanges.add(change(id));
            }
            afterArrival = new Thread(() -> {
                try {
                    CdcPhase.run(new FakeSource(List.of(), afterArrivalChanges), config(), chain,
                            () -> meta.consumerOffsets(chainId), new CaptureHealth());
                } catch (Throwable failure) {
                    afterArrivalFailure.set(failure);
                }
            }, "writer-after-arrival");
            afterArrival.setDaemon(true);
            afterArrival.start();

            long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
            while (ring.tailSequence() < 16L && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertThat(ring.tailSequence()).isEqualTo(16L);
            assertThat(afterArrival.isAlive())
                    .as("the capacity-plus-one change waits instead of overwriting the first owed change")
                    .isTrue();

            List<Integer> received = new ArrayList<>();
            SrsRingReader reader = SrsRingReader.resumingAfter(ring, 8L,
                    seq -> meta.advanceConsumerReadSeq(chainId, "pipe-joining", "orders", seq));
            assertThat(reader.fill((item, seq) -> received.add((Integer) item.after().get("id")), 1))
                    .isOne();
            afterArrival.join(2000);
            assertThat(afterArrival.isAlive()).isFalse();
            assertThat(afterArrivalFailure.get()).isNull();
            assertThat(reader.fill((item, seq) -> received.add((Integer) item.after().get("id")), 8))
                    .isEqualTo(8);
            assertThat(received).containsExactly(9, 10, 11, 12, 13, 14, 15, 16, 17);
        } finally {
            allowRegistration.countDown();
            meta.advanceConsumerReadSeq(chainId, "pipe-joining", "orders", Long.MAX_VALUE);
            joining.interrupt();
            if (afterArrival != null) {
                afterArrival.interrupt();
            }
        }
    }

    @Test
    void aPipelineComingBackKeepsThePlaceItHadInTheRingRatherThanTheOneTheRingHasReached() {
        InMemoryMeta meta = new InMemoryMeta();
        String chain = MiningChainId.resolve(config(), "chain-return").value();
        new CaptureRunUnit(new FakeSource(List.of(row(1)), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-return"), e -> { }, true);
        SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer(SrsRingbuffer.ringName(chain, "orders")));
        long hadReached = ring.append(buffered(1));
        meta.startRingAfter(chain, "pipe-b", "orders", hadReached);
        meta.advanceConsumerReadSeq(chain, "pipe-b", "orders", hadReached);
        ring.append(buffered(2));
        ring.append(buffered(3));

        new CaptureRunUnit(new FakeSource(List.of(row(1)), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-return"), e -> { }, false);

        assertThat(meta.ringDoneThrough(chain, "pipe-b"))
                .as("the two changes written while it was away are still owed to it")
                .containsExactly(Map.entry("orders", hadReached));
        assertThat(meta.read(chain).orElseThrow().consumerOffset("pipe-b").orElseThrow().perTableSeq())
                .as("attaching on another member leaves the returning reader's cursor advanced")
                .containsEntry("orders", hadReached);
    }

    @Test
    void aCdcOnlyReadFromThePresentIsMarkedAndOneFromTheEarliestChangeIsLeftToItsStart() {
        InMemoryMeta meta = new InMemoryMeta();
        String chain = MiningChainId.resolve(config(), "chain-cdc-start").value();
        new CaptureRunUnit(new FakeSource(List.of(row(1)), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-cdc-start"), e -> { }, true);
        SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer(SrsRingbuffer.ringName(chain, "orders")));
        long held = ring.append(buffered(1));

        new CaptureRunUnit(new FakeSource(List.of(), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(new CaptureRunSpec(config(), ReadMode.CDC_ONLY, "chain-cdc-start", true, "src-1",
                        "pipe-present", StartFrom.latest(), null, 0L), e -> { }, false);
        meta.advanceConsumerReadSeq(chain, "pipe-earliest", "orders", held);
        new CaptureRunUnit(new FakeSource(List.of(), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(new CaptureRunSpec(config(), ReadMode.CDC_ONLY, "chain-cdc-start", true, "src-1",
                        "pipe-earliest", StartFrom.earliest(), null, 0L), e -> { }, false);

        assertThat(meta.ringDoneThrough(chain, "pipe-present"))
                .as("a read from the present owes nothing the ring held before it arrived")
                .containsExactly(Map.entry("orders", held));
        assertThat(meta.ringDoneThrough(chain, "pipe-earliest"))
                .as("a read from the earliest change is owed everything the ring holds, so nothing is marked")
                .isEmpty();
        assertThat(meta.read(chain).orElseThrow().consumerOffset("pipe-present").orElseThrow().perTableSeq())
                .containsEntry("orders", held);
        assertThat(meta.read(chain).orElseThrow().consumerOffset("pipe-earliest").orElseThrow().perTableSeq())
                .containsEntry("orders", held);
    }

    private static SrsItem buffered(int id) {
        return new SrsItem(new SourcePosition("b" + id), Op.INSERT, 1L, null, Map.of("id", id), 0L);
    }

    @Test
    void routesAMultiTableSharedRingRunToOneSubscriptionAndTwoRings() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureConfig multi = new CaptureConfig("mysql", Map.of(), List.of("orders", "customers"));
        CaptureRunSpec spec = new CaptureRunSpec(multi, ReadMode.SNAPSHOT_AND_CDC, "k-multi", true, "src-1", "pipe-1",
                StartFrom.earliest(), null, 0L);
        List<Envelope> snapshots = List.of(
                Envelope.read(1, "orders", Map.of("id", 1), Map.of()),
                Envelope.read(2, "customers", Map.of("id", 2), Map.of()));
        List<Envelope> changes = List.of(
                Envelope.insert(1, "orders", Map.of("id", 1), Map.of()),
                Envelope.insert(2, "customers", Map.of("id", 2), Map.of()));
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(new FakeSource(snapshots, changes), meta).start(spec, passthrough::add);

        assertThat(run.chainId()).isPresent();
        assertThat(run.cdcSubscription()).isPresent();
        assertThat(run.snapshotCounts()).containsExactlyInAnyOrderEntriesOf(Map.of("orders", 1L, "customers", 1L));
        assertThat(passthrough).extracting(Envelope::src).containsExactly("orders", "customers");
        String chainId = run.chainId().orElseThrow().value();
        assertThat(hz.getRingbuffer(SrsRingbuffer.ringName(chainId, "orders")).tailSequence()).isEqualTo(0L);
        assertThat(hz.getRingbuffer(SrsRingbuffer.ringName(chainId, "customers")).tailSequence()).isEqualTo(0L);
        assertThat(meta.read(chainId).orElseThrow().consumerOffset("pipe-1").orElseThrow().perTableSeq())
                .containsExactlyInAnyOrderEntriesOf(Map.of("orders", -1L, "customers", -1L));
    }

    @Test
    void theReadCursorPublisherResolvesTheStoreMemberSideAndAdvancesTheConsumerCursor() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-pub", null);
        hz.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, meta);
        try {
            SrsReadCursorPublisherFactory factory =
                    CaptureRunUnit.readCursorPublisher("chain-pub", "pipe-7", "orders");

            factory.resolve(hz).accept(7L);

            // The factory holds only coordinates; resolved on the member it binds the store from the user
            // context and advances exactly this consumer's per-table cursor.
            ConsumerOffset offset = meta.read("chain-pub").orElseThrow().consumerOffsets().stream()
                    .filter(c -> c.pipelineId().equals("pipe-7")).findFirst().orElseThrow();
            assertThat(offset.perTableSeq()).containsEntry("orders", 7L);
        } finally {
            hz.getUserContext().remove(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY);
        }
    }

    @Test
    void readCursorPublishersAdvanceIndependentTableCursors() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-pub", null);
        hz.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, meta);
        try {
            CaptureRunUnit.readCursorPublisher("chain-pub", "pipe-7", "orders")
                    .resolve(hz).accept(7L);
            CaptureRunUnit.readCursorPublisher("chain-pub", "pipe-7", "customers")
                    .resolve(hz).accept(11L);

            ConsumerOffset offset = meta.read("chain-pub").orElseThrow().consumerOffsets().stream()
                    .filter(c -> c.pipelineId().equals("pipe-7")).findFirst().orElseThrow();
            assertThat(offset.perTableSeq()).containsExactlyInAnyOrderEntriesOf(Map.of(
                    "orders", 7L, "customers", 11L));
        } finally {
            hz.getUserContext().remove(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY);
        }
    }

    @Test
    void rejects_an_empty_stream_selection_before_provisioning() {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureRunSpec spec = new CaptureRunSpec(
                new CaptureConfig("mysql", Map.of("host", "h"), List.of()),
                ReadMode.CDC_ONLY, "chain-empty", true, "src-1", "pipe-1", StartFrom.earliest(), null, 0L);

        assertThatThrownBy(() -> runUnit(new FakeSource(List.of(), List.of()), meta)
                .start(spec, ignored -> { }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one stream");
        assertThat(meta.created).isEmpty();
    }

    @Test
    void snapshot_passthrough_failure_rolls_back_a_chain_created_by_this_start() {
        InMemoryMeta meta = new InMemoryMeta();
        SrsCoordinator coordinator = new SrsCoordinator(meta);
        FakeSource source = new FakeSource(List.of(row(1)), List.of());
        CaptureRunSpec spec = spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-snapshot-failure");
        MiningChainId chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
        RuntimeException failure = new IllegalStateException("snapshot sink failed");
        CaptureRunUnit unit = new CaptureRunUnit(source, coordinator, meta, hz);

        assertThatThrownBy(() -> unit.start(spec, ignored -> { throw failure; }))
                .isSameAs(failure);

        assertThat(coordinator.isProvisioned(chainId)).isFalse();
        assertThat(source.cdcStarted).isFalse();
    }

    @Test
    void theReadCursorPublisherResolvesToANoOpWhenNoStoreIsBoundOnTheMember() {
        hz.getUserContext().remove(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY);

        // No store bound on the member: the factory resolves to a no-op sink, so a source still runs before
        // the assembly layer makes the member SRS-capable. Resolving and calling it does not throw.
        SrsReadCursorPublisherFactory factory = CaptureRunUnit.readCursorPublisher("chain-x", "pipe-x", "orders");
        factory.resolve(hz).accept(3L);
    }

    /**
     * What a run of changes costs the coordination record must not grow with what the record has
     * accumulated. Both bounds a run applies -- how far ahead of its readers the ring may be written, and
     * how far the durable read offset may advance -- are functions of the consumer cursors alone, so a run
     * asks for those and nothing else.
     *
     * <p>The record also carries a schema history that grows by one entry per DDL, up to the bound the
     * store keeps it under. Fetching the whole record per run therefore carries that history back on every
     * change, and the cost of doing so climbs with what the record holds: measured against a real endpoint,
     * a chain with 500 DDLs behind it reads at 6.4 ms where the cursors alone read at 0.5 ms.
     *
     * <p>So this pins two things at once, and the second is the one that would rot silently: a run reads
     * the cursors once rather than once per bound, and the number of whole-record fetches does not move
     * when the number of runs does.
     */
    @Test
    void aRunOfChangesReadsTheCursorsOnceAndNeverFetchesTheWholeRecord() {
        InMemoryMeta few = new InMemoryMeta();
        runUnit(new FakeSource(List.of(), List.of(change(10), change(11))), few)
                .start(spec(ReadMode.CDC_ONLY, true, "chain-reads-few"), e -> { });
        InMemoryMeta many = new InMemoryMeta();
        runUnit(new FakeSource(List.of(), List.of(
                        change(10), change(11), change(12), change(13), change(14),
                        change(15), change(16), change(17))),
                many)
                .start(spec(ReadMode.CDC_ONLY, true, "chain-reads-many"), e -> { });

        // One cursor read per run of changes -- not two, which is what asking for each bound separately
        // costs when both come from the same record.
        assertThat(many.cursorReads - few.cursorReads)
                .as("cursor reads scale one-for-one with runs of changes")
                .isEqualTo(6);
        // And the whole record is fetched only by the start path, the same number of times either way:
        // six more runs of changes fetch it not once more.
        assertThat(many.wholeRecordReads)
                .as("whole-record fetches do not scale with the number of change runs")
                .isEqualTo(few.wholeRecordReads);
    }

    @Test
    void theHeadroomBoundIsTheSlowestCursorAcrossTheChainsConsumers() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-min", null);
        // Two consumers on the chain: one has read orders up to 5, the other only to 2 -- the slowest bounds it.
        meta.advanceConsumerReadSeq("chain-min", "p1", "orders", 5L);
        meta.advanceConsumerReadSeq("chain-min", "p2", "orders", 2L);

        assertThat(CdcPhase.headroomBound(meta.consumerOffsets("chain-min"), "orders")).isEqualTo(2L);
    }

    @Test
    void theHeadroomBoundIsUnconstrainedWhenNoConsumerHasACursorYet() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-none", null);

        // No consumer has published a cursor: nothing constrains the ring, so the write gate sees no bound.
        assertThat(CdcPhase.headroomBound(meta.consumerOffsets("chain-none"), "orders"))
                .isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void theHeadroomBoundIgnoresAConsumerWithoutThatTable() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-other", null);
        // A cursor for customers alone does not say this consumer subscribes to orders.
        meta.advanceConsumerReadSeq("chain-other", "p1", "customers", 9L);

        assertThat(CdcPhase.headroomBound(meta.consumerOffsets("chain-other"), "orders"))
                .isEqualTo(Long.MAX_VALUE);
    }

    /** A mock connector: a fixed snapshot batch and a fixed change stream driven into the listener when cdc starts. */
    private static final class FakeSource implements CapturePort {
        private final List<Envelope> snapshotRows;
        private final List<Envelope> changes;
        /** The position this source samples before its bounded read -- where a tail of it must join. */
        private final String seam;
        private Throwable cdcError;
        boolean cdcStarted;
        int cdcStarts;
        /** Where the run asked this source to begin -- the whole of what a resume is observable as. */
        CaptureStart cdcStart;
        CaptureConfig cdcConfig;
        boolean cdcClosed;
        /** Every position the run told this source it may release up to, in order. */
        final List<String> acknowledged = new java.util.concurrent.CopyOnWriteArrayList<>();
        /** Every config the run asked this source to let go of what it set up through, in order. */
        final List<CaptureConfig> released = new java.util.concurrent.CopyOnWriteArrayList<>();
        /** What this source answers a release with. */
        Optional<TapstateException> refusal = Optional.empty();

        FakeSource(List<Envelope> snapshotRows, List<Envelope> changes) {
            this(snapshotRows, changes, "seam-0");
        }

        /** A source whose bounded read samples a named seam, so two runs of one chain can differ in it. */
        FakeSource(List<Envelope> snapshotRows, List<Envelope> changes, String seam) {
            this.snapshotRows = snapshotRows;
            this.changes = changes;
            this.seam = seam;
        }

        /** Makes this source's cdc stream report a failure through the listener rather than deliver changes. */
        FakeSource failing(Throwable error) {
            this.cdcError = error;
            return this;
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            // A bounded read yields the rows of the streams it selected and no others; an empty selection
            // is every stream the source exposes. The snapshot phase reads one table at a time, so a double
            // that ignored the selection would answer each of those reads with the whole source.
            List<String> selected = config.streams();
            return new FakeBatch(selected.isEmpty() ? snapshotRows
                    : snapshotRows.stream().filter(row -> selected.contains(row.src())).toList(), seam);
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            cdcConfig = config;
            cdcStarted = true;
            cdcStarts++;
            cdcStart = start;
            if (cdcError != null) {
                listener.onError(cdcError);
                return subscription();
            }
            for (Envelope e : changes) {
                listener.onBatch(java.util.List.of(e), Optional.of(new SourcePosition("src-" + e.ts())));
            }
            return subscription();
        }

        private Subscription subscription() {
            return new Subscription() {
                @Override
                public void acknowledge(SourcePosition durable) {
                    acknowledged.add(durable.token());
                }

                @Override
                public void close() {
                    cdcClosed = true;
                }
            };
        }

        @Override
        public ConnectionReport testConnection(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DiscoveredSchema discoverSchema(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<TapstateException> release(CaptureConfig config) {
            released.add(config);
            return refusal;
        }
    }

    /**
     * A source whose load is a fixed list of rows and whose tail is whatever {@code tail} opens -- for a case
     * about what happens around the tail's opening rather than about the changes it delivers.
     */
    private static final class LoadThenTailSource implements CapturePort {
        private final List<Envelope> rows;
        private final Supplier<Subscription> tail;
        /** Where the run asked the tail to begin. */
        volatile CaptureStart cdcStart;

        LoadThenTailSource(List<Envelope> rows, Supplier<Subscription> tail) {
            this.rows = rows;
            this.tail = tail;
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            return new FakeBatch(rows, "seam-0");
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            cdcStart = start;
            return tail.get();
        }

        @Override
        public ConnectionReport testConnection(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DiscoveredSchema discoverSchema(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }
    }

    /** A bounded snapshot batch over a fixed list of events. */
    private static final class FakeBatch implements CaptureBatch {
        private final Iterator<Envelope> events;
        private final String seam;

        FakeBatch(List<Envelope> events, String seam) {
            this.events = events.iterator();
            this.seam = seam;
        }

        @Override
        public boolean hasNext() {
            return events.hasNext();
        }

        @Override
        public Envelope next() {
            return events.next();
        }

        @Override
        public Optional<SourcePosition> seam() {
            // The source sampled this before reading its first row; the run under test refuses to start a
            // tail without one, because a tail that begins wherever it likes loses every change made while
            // the snapshot ran.
            return Optional.of(new SourcePosition(seam));
        }

        @Override
        public void close() {
        }
    }

    /**
     * A faithful in-memory {@link SrsMetaStore}: insert-only create, per-facet mutators that reject an
     * unseeded chain, and a read-cursor advance that upserts one consumer's {@code perTableSeq} without
     * clobbering its sink-ack — enough to exercise the run unit's provision, cdc-start, offset and cursor
     * wiring without a store backend.
     */
    static class InMemoryMeta implements SrsMetaStore {
        /** Per chain and pipeline, how far each table's ring is done with -- kept once, never raised here. */
        final Map<String, Map<String, Long>> ringDone = new LinkedHashMap<>();
        private volatile String pausedPipeline;
        private volatile CountDownLatch registrationReached;
        private volatile CountDownLatch allowRegistration;

        void pauseRegistration(String pipelineId, CountDownLatch reached, CountDownLatch allow) {
            pausedPipeline = pipelineId;
            registrationReached = reached;
            allowRegistration = allow;
        }

        @Override
        public synchronized void startRingAfter(String miningChainId, String pipelineId, String table, long seq) {
            Long done = ringDone.computeIfAbsent(miningChainId + "/" + pipelineId,
                            key -> new LinkedHashMap<>())
                    .putIfAbsent(table, seq);
            if (done == null) {
                advanceConsumerReadSeqNow(miningChainId, pipelineId, table, seq);
            }
        }

        @Override
        public synchronized Map<String, Long> ringDoneThrough(String miningChainId, String pipelineId) {
            return Map.copyOf(ringDone.getOrDefault(miningChainId + "/" + pipelineId, Map.of()));
        }

        @Override
        public java.util.List<String> miningChainIdsWithConsumer(String pipelineId) {
            throw new UnsupportedOperationException("consumer detachment is not exercised by this double");
        }

        @Override
        public void dropChain(String miningChainId) {
            throw new UnsupportedOperationException(
                    "chain removal is not exercised by this double");
        }

        @Override
        public void detachConsumer(String miningChainId, String pipelineId) {
            SrsMeta m = records.get(miningChainId);
            if (m == null) {
                return;
            }
            List<ConsumerOffset> kept = m.consumerOffsets().stream()
                    .filter(c -> !c.pipelineId().equals(pipelineId))
                    .toList();
            records.put(miningChainId, new SrsMeta(m.miningChainId(), m.sourceRead(), kept,
                    m.schemaHistory(), m.retention(), m.epoch(), m.sourceReadAt(), m.sourceReadDurable()));
        }

        final List<String> created = new ArrayList<>();
        /** How often the whole record was fetched, and how often the cursors alone were. */
        int wholeRecordReads;
        int cursorReads;
        private final Map<String, SrsMeta> records = new LinkedHashMap<>();

        /** Puts {@code record} in place as it stands, for a case that needs a field no write here sets. */
        synchronized void seed(SrsMeta record) {
            records.put(record.miningChainId(), record);
        }

        @Override
        public synchronized Optional<SrsMeta> read(String miningChainId) {
            wholeRecordReads++;
            return Optional.ofNullable(records.get(miningChainId));
        }

        @Override
        public synchronized List<ConsumerOffset> consumerOffsets(String miningChainId) {
            cursorReads++;
            Optional<SrsMeta> record = read(miningChainId);
            // This double answers the narrow read out of the same map, so the line above counted a whole
            // record fetch that a real store would not have made. Take it back off: what this counter is
            // for is fetches made for their own sake.
            wholeRecordReads--;
            return record.map(SrsMeta::consumerOffsets).orElse(List.of());
        }

        @Override
        public void create(String miningChainId, String retention) {
            if (records.containsKey(miningChainId)) {
                throw new IllegalStateException("mining chain already seeded: " + miningChainId);
            }
            created.add(miningChainId);
            records.put(miningChainId, new SrsMeta(miningChainId, null, List.of(), List.of(), retention));
        }

        @Override
        public void rewindSourceReadOffset(String miningChainId, String token) {
            // No test on this double writes a position back; a call here is a wiring mistake, not a case.
            throw new UnsupportedOperationException("rewindSourceReadOffset");
        }

        @Override
        public void advanceSourceReadOffset(String miningChainId, ChainPosition position) {
            SrsMeta m = require(miningChainId);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), position, m.consumerOffsets(),
                    m.schemaHistory(), m.retention(), m.epoch()));
        }

        @Override
        public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
            SrsMeta m = require(miningChainId);
            List<ConsumerOffset> next = new ArrayList<>(m.consumerOffsets());
            next.removeIf(c -> c.pipelineId().equals(offset.pipelineId()));
            next.add(offset);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), next,
                    m.schemaHistory(), m.retention(), m.epoch(), m.sourceReadAt(), m.sourceReadDurable()));
        }

        @Override
        public void advanceConsumerReadSeq(String miningChainId, String pipelineId, String table, long lastReadSeq) {
            if (lastReadSeq == -1L && pipelineId.equals(pausedPipeline)) {
                registrationReached.countDown();
                awaitRoom(allowRegistration);
            }
            synchronized (this) {
                advanceConsumerReadSeqNow(miningChainId, pipelineId, table, lastReadSeq);
            }
        }

        private void advanceConsumerReadSeqNow(
                String miningChainId, String pipelineId, String table, long lastReadSeq) {
            SrsMeta m = require(miningChainId);
            List<ConsumerOffset> next = new ArrayList<>();
            ConsumerOffset existing = null;
            for (ConsumerOffset c : m.consumerOffsets()) {
                if (c.pipelineId().equals(pipelineId)) {
                    existing = c;
                } else {
                    next.add(c);
                }
            }
            Map<String, Long> perTable = new LinkedHashMap<>(existing == null ? Map.of() : existing.perTableSeq());
            perTable.merge(table, lastReadSeq, Math::max);
            ChainPosition ack = existing == null ? null : existing.sinkAcked();
            next.add(new ConsumerOffset(
                    pipelineId,
                    perTable,
                    ack,
                    existing == null ? List.of() : existing.snapshotCompletedTables(),
                    existing == null ? null : existing.cdcStartPosition(),
                    existing == null ? 0L : existing.snapshotEpoch()));
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), next,
                    m.schemaHistory(), m.retention(), m.epoch(), m.sourceReadAt(), m.sourceReadDurable()));
        }

        @Override
        public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
            SrsMeta m = require(miningChainId);
            List<ConsumerOffset> next = new ArrayList<>();
            ConsumerOffset existing = null;
            for (ConsumerOffset c : m.consumerOffsets()) {
                if (c.pipelineId().equals(pipelineId)) {
                    existing = c;
                } else {
                    next.add(c);
                }
            }
            Map<String, Long> perTable = existing == null ? Map.of() : existing.perTableSeq();
            next.add(new ConsumerOffset(
                    pipelineId,
                    perTable,
                    position,
                    existing == null ? List.of() : existing.snapshotCompletedTables(),
                    existing == null ? null : existing.cdcStartPosition(),
                    existing == null ? 0L : existing.snapshotEpoch()));
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), next,
                    m.schemaHistory(), m.retention(), m.epoch(), m.sourceReadAt(), m.sourceReadDurable()));
        }

        @Override
        public void setCdcStart(
                String miningChainId, String pipelineId, String cdcStartPosition, long snapshotEpoch) {
            SrsMeta m = require(miningChainId);
            List<ConsumerOffset> next = new ArrayList<>();
            ConsumerOffset existing = null;
            for (ConsumerOffset consumer : m.consumerOffsets()) {
                if (consumer.pipelineId().equals(pipelineId)) {
                    existing = consumer;
                } else {
                    next.add(consumer);
                }
            }
            next.add(new ConsumerOffset(
                    pipelineId,
                    existing == null ? Map.of() : existing.perTableSeq(),
                    existing == null ? null : existing.sinkAcked(),
                    existing == null ? List.of() : existing.snapshotCompletedTables(),
                    cdcStartPosition,
                    snapshotEpoch));
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), next,
                    m.schemaHistory(), m.retention(), m.epoch(), m.sourceReadAt(), m.sourceReadDurable()));
        }

        @Override
        public long openEpoch(String miningChainId) {
            SrsMeta m = require(miningChainId);
            long opened = m.epoch() + 1;
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), m.consumerOffsets(),
                    m.schemaHistory(), m.retention(), opened, m.sourceReadAt(), m.sourceReadDurable()));
            return opened;
        }

        @Override
        public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
            SrsMeta m = require(miningChainId);
            List<SchemaVersion> next = new ArrayList<>(m.schemaHistory());
            next.add(version);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), m.consumerOffsets(),
                    next, m.retention(), m.epoch(), m.sourceReadAt(), m.sourceReadDurable()));
        }

        @Override
        public void markSnapshotComplete(String miningChainId, String pipelineId, String table) {
            SrsMeta m = require(miningChainId);
            // Per pipeline, not per chain: the mark says this pipeline's sink took the table, and the
            // pipelines sharing a chain each write somewhere of their own.
            List<ConsumerOffset> consumers = new ArrayList<>();
            ConsumerOffset mine = null;
            for (ConsumerOffset consumer : m.consumerOffsets()) {
                if (consumer.pipelineId().equals(pipelineId)) {
                    mine = consumer;
                } else {
                    consumers.add(consumer);
                }
            }
            List<String> completed =
                    new ArrayList<>(mine == null ? List.of() : mine.snapshotCompletedTables());
            if (!completed.contains(table)) {
                completed.add(table);
            }
            consumers.add(new ConsumerOffset(pipelineId, mine == null ? Map.of() : mine.perTableSeq(),
                    mine == null ? null : mine.sinkAcked(), completed,
                    mine == null ? null : mine.cdcStartPosition(),
                    mine == null ? 0L : mine.snapshotEpoch()));
            records.put(miningChainId, new SrsMeta(m.miningChainId(), m.sourceRead(), consumers,
                    m.schemaHistory(), m.retention(), m.epoch(), m.sourceReadAt(), m.sourceReadDurable()));
        }

        private SrsMeta require(String miningChainId) {
            SrsMeta m = records.get(miningChainId);
            if (m == null) {
                throw new IllegalStateException("mining chain not seeded: " + miningChainId);
            }
            return m;
        }
    }

    /**
     * A direct tail with nothing recorded yet begins where its author asked. {@code start_from} is that
     * ask, and on this path it names a position in the source's own log rather than a cursor into a replay
     * buffer -- there is no buffer here for it to point into.
     *
     * <p>The three forms are not interchangeable: {@code earliest} asks for the oldest change the source
     * still retains, an instant asks the source to resolve that moment to a position of its own, and
     * {@code latest} asks for only what is written from now on. Collapsing any of them into the present is
     * the silent form of ignoring the setting -- the tail comes up healthy having read a different stretch
     * than the one asked for, which is the same failure a start clamped to a buffer's head makes.
     */
    @Test
    void aDirectTailWithNothingRecordedBeginsWhereItsAuthorAsked() {
        Instant asked = Instant.parse("2026-09-01T00:00:00Z");

        assertThat(directTailStart(StartFrom.earliest(), "chain-first-earliest"))
                .as("earliest asks the source for the oldest change it still retains")
                .isEqualTo(CaptureStart.earliest());
        assertThat(directTailStart(StartFrom.at(asked), "chain-first-at"))
                .as("an instant is a start only the source can resolve to a position of its own")
                .isEqualTo(CaptureStart.at(asked));
        assertThat(directTailStart(StartFrom.latest(), "chain-first-latest"))
                .as("latest is the present moment: only changes written from now on")
                .isEqualTo(CaptureStart.present());
    }

    /**
     * A recorded position outranks {@code start_from} on a direct tail. The setting says where a read
     * begins, not where every later run of it begins: honoured again on the way back it would re-read the
     * stretch already read on every restart, and asking for the whole source again is a separate request
     * with its own verb.
     *
     * <p>This is what makes the case above a statement about a first run rather than about the setting
     * always winning, and the two readings are distinguishable only here: with nothing recorded they give
     * the same answer.
     */
    @Test
    void aRecordedPositionOutranksStartFromOnADirectTail() {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureRunSpec spec = spec(ReadMode.CDC_ONLY, false, "chain-start-from-outranked", StartFrom.earliest());
        MiningChainId chainId = spec.miningChainId();
        meta.create(chainId.value(), null);
        meta.advanceSourceReadOffset(chainId.value(), new ChainPosition(new SourceOrder(1L, 7L), "src-11"));

        FakeSource port = new FakeSource(List.of(), List.of());
        runUnit(port, meta).start(spec, e -> { });

        assertThat(port.cdcStart)
                .as("the position the last run reached wins; start_from named where the first one began")
                .isEqualTo(CaptureStart.resume(new SourcePosition("src-11")));
    }

    @Test
    void loadingANewTableWithALegacySeamKeepsADirectTailsExistingTableChanges() {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureConfig config = new CaptureConfig("mysql", Map.of("host", "h"), List.of("orders", "customers"));
        CaptureRunSpec spec = new CaptureRunSpec(config, ReadMode.SNAPSHOT_AND_CDC, "chain-legacy-added-table",
                false, "src-1", "pipe-1", StartFrom.earliest(), null, 0L);
        String chain = spec.miningChainId().value();
        String firstSeam = "seam-of-the-first-load";
        ChainPosition landed = new ChainPosition(new SourceOrder(1L, 7L), "orders-landed-up-to-here");
        // A v0.1.0 record has a seam but no snapshot generation; orders are already confirmed.
        meta.seed(new SrsMeta(chain, landed,
                List.of(new ConsumerOffset("pipe-1", Map.of("orders", 7L), landed,
                        List.of("orders"), firstSeam, 0L)),
                List.of(), null, 1L));
        FakeSource source = new FakeSource(List.of(row(1),
                Envelope.read(2, "customers", Map.of("id", 2), Map.of())),
                List.of(), "seam-of-the-customers-load");
        List<Envelope> delivered = new ArrayList<>();

        try (CaptureRun run = runUnit(source, meta).start(spec, delivered::add)) {
            assertThat(run.snapshotCount()).isEqualTo(1L);
            assertThat(delivered).extracting(Envelope::src).containsExactly("customers");
            assertThat(source.cdcStart)
                    .as("loading only customers must not move the direct tail past the saved orders checkpoint")
                    .isIn(CaptureStart.resume(new SourcePosition(landed.token())),
                            CaptureStart.resume(new SourcePosition(firstSeam)));
        }
    }

    /**
     * A buffered tail's miner does not take {@code start_from}, because on that path the setting is this
     * one pipeline's cursor into the shared buffer and the miner is shared by all of them. The buffer is
     * mined once and each consumer finds its own start in it, so a miner that honoured one consumer's ask
     * would move where every other consumer's changes came from.
     *
     * <p>This is the control on the case above: it is what separates "the direct path resolves the ask"
     * from "the ask is resolved on every path", and only the buffered side can tell the two apart.
     */
    @Test
    void aBufferedTailsMinerDoesNotTakeStartFrom() {
        FakeSource port = new FakeSource(List.of(), List.of());
        runUnit(port, new InMemoryMeta()).start(
                spec(ReadMode.CDC_ONLY, true, "chain-miner-ignores-start-from", StartFrom.latest()), e -> { });

        assertThat(port.cdcStart)
                .as("the miner begins at the present with nothing recorded, whatever a consumer asked for")
                .isEqualTo(CaptureStart.present());
    }

    /**
     * A buffered tail refuses a past instant when this run is the one that starts the mining. Nothing has
     * been mined yet and nothing from before this moment ever will be, so the ask cannot be met -- and the
     * reader cannot see that for itself: an empty ring is the same shape whether its oldest change has
     * aged out or has simply not arrived, which is why the refusal belongs here, where the miner's own
     * start is decided.
     *
     * <p>Served rather than refused it is silent, and the silence is the whole of the defect: the pipeline
     * comes up healthy, reports running, and reads only what is written from now on -- every change between
     * the instant asked for and the moment the run came up is gone, with nothing thrown and nothing logged.
     */
    @Test
    void aBufferedTailRefusesAPastInstantWhenThisRunIsWhatStartsTheMining() {
        FakeSource port = new FakeSource(List.of(), List.of());

        TapstateException refused = catchThrowableOfType(() -> runUnit(port, new InMemoryMeta()).start(
                spec(ReadMode.CDC_ONLY, true, "chain-fresh-past-instant",
                        StartFrom.at(Instant.parse("2020-01-01T00:00:00Z"))), e -> { }),
                TapstateException.class);

        assertThat(refused.code()).isEqualTo(CaptureError.START_FROM_OUTSIDE_WINDOW);
        assertThat(refused.args())
                .as("the refusal names what was asked for and how far back this buffer goes")
                .containsEntry("requested", "2020-01-01T00:00:00Z")
                .containsEntry("retention", "unset")
                .containsKey("earliest");
        assertThat(port.cdcStarted)
                .as("it refuses before opening the source's stream, not after")
                .isFalse();
    }

    private static HazelcastInstance emptyDurableMember() {
        SrsLogStore log = new SrsLogStore() {
            @Override
            public void store(String ring, long sequence, SrsLogRecord record) {
                throw new AssertionError("an empty capture must not append a change");
            }

            @Override
            public void storeAll(String ring, long sequence, List<SrsLogRecord> records) {
                throw new AssertionError("an empty capture must not append a batch");
            }

            @Override
            public Optional<SrsLogRecord> load(String ring, long sequence) {
                return Optional.empty();
            }

            @Override
            public long largestSequence(String ring) {
                return -1L;
            }

            @Override
            public void trim(String ring, long sequence) {
                throw new AssertionError("an empty log has no confirmed history to trim");
            }
        };
        Config memberConfig = new Config();
        memberConfig.setClusterName("srs-durable-instant-test-" + System.nanoTime());
        memberConfig.setProperty("hazelcast.phone.home.enabled", "false");
        memberConfig.setProperty("hazelcast.shutdownhook.enabled", "false");
        memberConfig.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        memberConfig.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        memberConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        memberConfig.getJetConfig().setEnabled(false);
        memberConfig.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(8)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setBackupCount(0)
                .setRingbufferStoreConfig(new RingbufferStoreConfig().setEnabled(true)
                        .setFactoryImplementation(new SrsLogRingbufferStoreFactory(log))));
        HazelcastInstance durableMember = Hazelcast.newHazelcastInstance(memberConfig);
        durableMember.getUserContext().put(CaptureRunUnit.SRS_LOG_USER_CONTEXT_KEY, log);
        return durableMember;
    }

    @Test
    void aFreshDurableChainRefusesAPastInstantAndAcceptsAReachableInstant() {
        HazelcastInstance durableMember = emptyDurableMember();
        try {
            FakeSource past = new FakeSource(List.of(), List.of());
            InMemoryMeta pastMeta = new InMemoryMeta();
            CaptureRunUnit pastUnit = new CaptureRunUnit(past, new SrsCoordinator(pastMeta), pastMeta,
                    durableMember);

            TapstateException refused = catchThrowableOfType(() -> pastUnit.start(
                    spec(ReadMode.CDC_ONLY, true, "chain-durable-past-instant",
                            StartFrom.at(Instant.parse("2020-01-01T00:00:00Z"))), e -> { }),
                    TapstateException.class);

            assertThat(refused.code()).isEqualTo(CaptureError.START_FROM_OUTSIDE_WINDOW);
            assertThat(refused.args()).containsEntry("requested", "2020-01-01T00:00:00Z")
                    .containsEntry("retention", "unset").containsKey("earliest");
            assertThat(past.cdcStarted).isFalse();

            FakeSource reachable = new FakeSource(List.of(), List.of());
            InMemoryMeta reachableMeta = new InMemoryMeta();
            CaptureRunSpec reachableSpec = spec(ReadMode.CDC_ONLY, true, "chain-durable-reachable-instant",
                    StartFrom.at(Instant.now().plusSeconds(3600)));
            String chain = reachableSpec.miningChainId().value();
            reachableMeta.create(chain, null);
            String previousConsumer = SrsConsumerId.of("previous_pipeline", "previous_source").value();
            String directConsumer = SrsConsumerId.of("direct_pipeline", "direct_source").value();
            reachableMeta.upsertConsumerOffset(chain, new ConsumerOffset(previousConsumer, Map.of("orders", -1L),
                    null, List.of(), null, 0L, Map.of(), ConsumerProgressKind.SRS));
            reachableMeta.upsertConsumerOffset(chain, new ConsumerOffset(directConsumer, Map.of("orders", -1L),
                    null, List.of(), null, 0L, Map.of(), ConsumerProgressKind.DIRECT_SOURCE));
            reachableMeta.upsertConsumerOffset(chain, new ConsumerOffset("unknown_previous_pipeline",
                    Map.of("orders", -1L), null));
            CaptureRunUnit reachableUnit = new CaptureRunUnit(reachable, new SrsCoordinator(reachableMeta),
                    reachableMeta, durableMember);
            CaptureRun run = reachableUnit.start(reachableSpec, e -> { });
            try {
                assertThat(reachable.cdcStarted).isTrue();
                assertThat(reachable.cdcStart).isEqualTo(CaptureStart.present());
                assertThat(reachable.cdcConfig.node()).isEqualTo(reachableSpec.config().node());
                assertThat(reachable.cdcConfig.sharedNotes().sharedBy()).isEqualTo(chain);
                assertThat(reachable.cdcConfig.sharedNotes().carriedFrom()).containsExactly(
                        reachableSpec.config().node(), new PipelineNode("previous_pipeline", "previous_source"));
            } finally {
                run.close();
            }
        } finally {
            durableMember.shutdown();
        }
    }

    @Test
    void aSharedCdcOnlyProducerRestartRequiresItsAnchorWhileLiveAndRemoteAttachmentsKeepFollowing() {
        HazelcastInstance member = emptyDurableMember();
        try {
            InMemoryMeta meta = new InMemoryMeta();
            String key = "chain-scoped-unanchored-cdc";
            CaptureRunSpec owner = new CaptureRunSpec(config(), ReadMode.CDC_ONLY, key, true,
                    "root_source", "root_pipeline", StartFrom.latest(), null, 0L)
                    .withConsumerId(SrsConsumerId.of("root_pipeline", "root_source").value());
            CaptureRunSpec joining = new CaptureRunSpec(config(), ReadMode.CDC_ONLY, key, true,
                    "mail_source", "mail_pipeline", StartFrom.latest(), null, 0L)
                    .withConsumerId(SrsConsumerId.of("mail_pipeline", "mail_source").value());
            CaptureRunSpec remoteSpec = new CaptureRunSpec(config(), ReadMode.CDC_ONLY, key, true,
                    "remote_source", "remote_pipeline", StartFrom.latest(), null, 0L)
                    .withConsumerId(SrsConsumerId.of("remote_pipeline", "remote_source").value());
            String chain = owner.miningChainId().value();
            meta.create(chain, null);
            assertThat(meta.read(chain).orElseThrow().epoch()).isZero();
            FakeSource source = new FakeSource(List.of(), List.of());
            CaptureRunUnit producer = new CaptureRunUnit(source, new SrsCoordinator(meta), meta, member);
            CaptureRun initial = producer.start(owner, e -> { });
            CaptureRun local = producer.start(joining, e -> { });
            FakeSource remoteSource = new FakeSource(List.of(), List.of());
            CaptureRun remote = new CaptureRunUnit(remoteSource, new SrsCoordinator(meta), meta, member)
                    .start(remoteSpec, e -> { }, false);
            try {
                assertThat(source.cdcStarts).as("a fresh chain and a live attachment share one physical read")
                        .isOne();
                assertThat(remoteSource.cdcStarted).isFalse();
                assertThat(meta.read(chain).orElseThrow().sourceReadOffset()).isNull();
            } finally {
                remote.close();
                local.close();
                initial.close();
            }
            long retainedEpoch = meta.read(chain).orElseThrow().epoch();
            meta.setCdcStart(chain, joining.consumerId(), "another-nodes-seam", retainedEpoch);
            FakeSource restarted = new FakeSource(List.of(), List.of());
            CaptureRunUnit replacement = new CaptureRunUnit(restarted, new SrsCoordinator(meta), meta, member);

            TapstateException failure = catchThrowableOfType(() -> replacement.start(owner, e -> { }),
                    TapstateException.class);

            assertThat(failure.code()).isEqualTo(CaptureError.RECOVERY_PROGRESS_UNPROVEN);
            assertThat(failure.args()).containsEntry("pipeline", owner.pipelineId())
                    .containsEntry("source", owner.sourceId());
            assertThat(restarted.cdcStarted).isFalse();
            assertThat(meta.read(chain).orElseThrow().epoch()).isEqualTo(retainedEpoch);
            assertThat(meta.read(chain).orElseThrow().sourceReadOffset()).isNull();
            assertThat(meta.ringDoneThrough(chain, owner.consumerId())).containsEntry("orders", -1L);

            meta.setCdcStart(chain, owner.consumerId(), "this-nodes-proven-snapshot-seam", retainedEpoch);
            try (CaptureRun resumed = replacement.start(owner, e -> { })) {
                assertThat(restarted.cdcStart)
                        .isEqualTo(CaptureStart.resume(new SourcePosition("this-nodes-proven-snapshot-seam")));
            }
        } finally {
            member.shutdown();
        }
    }

    @Test
    void anIndependentChannelRefusesLegacyProgressStillStoredUnderTheSharedChain() {
        for (ReadMode mode : List.of(ReadMode.SNAPSHOT_AND_CDC, ReadMode.CDC_ONLY)) {
            InMemoryMeta meta = new InMemoryMeta();
            CaptureRunSpec direct = spec(mode, false, "legacy-shared-" + mode.name(), StartFrom.latest())
                    .withConsumerId(SrsConsumerId.of("pipe-1", "src-1").value());
            String shared = MiningChainId.resolve(direct.config(), direct.srsKey()).value();
            meta.create(shared, null);
            meta.advanceConsumerReadSeq(shared, direct.pipelineId(), "orders", 7);
            meta.setCdcStart(shared, direct.pipelineId(), "legacy-seam", 1);
            SrsMeta retained = meta.read(shared).orElseThrow();
            FakeSource source = new FakeSource(List.of(), List.of());

            TapstateException failure = catchThrowableOfType(
                    () -> runUnit(source, meta).start(direct, event -> { }), TapstateException.class);

            assertThat(failure.code()).isEqualTo(CaptureError.RECOVERY_PROGRESS_UNPROVEN);
            assertThat(source.cdcStarted).isFalse();
            assertThat(meta.read(shared)).contains(retained);
            assertThat(meta.read(direct.miningChainId().value())).isEmpty();
        }
    }

    /**
     * The control on the case above: an instant this buffer will still cover is taken, not refused. Mining
     * begins now, so a moment at or after that is reachable by waiting rather than unreachable, and an
     * implementation that refused every instant on a fresh chain satisfies the case above while making the
     * setting unusable on exactly the path it was extended for.
     */
    @Test
    void aBufferedTailTakesAnInstantThisBufferWillStillCover() {
        FakeSource port = new FakeSource(List.of(), List.of());

        runUnit(port, new InMemoryMeta()).start(
                spec(ReadMode.CDC_ONLY, true, "chain-fresh-reachable-instant",
                        StartFrom.at(Instant.now().plusSeconds(3600))), e -> { });

        assertThat(port.cdcStarted)
                .as("nothing before the mining begins is missed, so there is nothing to refuse")
                .isTrue();
    }

    /**
     * A chain with a position to resume from is not refused, whatever instant was asked for. How far back
     * its buffer will reach is that recorded position -- the source's own opaque token, which says nothing
     * about a moment -- so the reachability this refusal turns on is not knowable here, and refusing on a
     * guess would fail runs that were going to be served.
     *
     * <p>This is what makes the refusal a statement about a chain whose mining starts at the present, and
     * not about past instants in general. The two readings agree everywhere except here.
     */
    @Test
    void aChainWithAPositionToResumeFromIsNotRefusedWhateverWasAskedFor() {
        InMemoryMeta meta = new InMemoryMeta();
        MiningChainId chainId = MiningChainId.resolve(config(), "chain-resumed-past-instant");
        meta.create(chainId.value(), null);
        meta.advanceSourceReadOffset(chainId.value(), new ChainPosition(new SourceOrder(1L, 7L), "src-11"));

        FakeSource port = new FakeSource(List.of(), List.of());
        runUnit(port, meta).start(
                spec(ReadMode.CDC_ONLY, true, "chain-resumed-past-instant",
                        StartFrom.at(Instant.parse("2020-01-01T00:00:00Z"))), e -> { });

        assertThat(port.cdcStart)
                .as("the miner resumes; where its buffer reaches back to is not a moment this can compare")
                .isEqualTo(CaptureStart.resume(new SourcePosition("src-11")));
    }

    /**
     * Where a direct tail with nothing recorded asks its source to begin, given one {@code start_from}.
     * A fresh store per call is what makes it a first run; the key keeps each call's chain its own.
     */
    private CaptureStart directTailStart(StartFrom startFrom, String srsKey) {
        FakeSource port = new FakeSource(List.of(), List.of());
        runUnit(port, new InMemoryMeta())
                .start(spec(ReadMode.CDC_ONLY, false, srsKey, startFrom), e -> { });
        return port.cdcStart;
    }

    /**
     * A capture whose rings write through tells its source the checkpoint it resumes from the moment its
     * stream is open: the source may let go of everything before it, because each consumer replays what it has
     * not landed from the recoverable log rather than from the source.
     */
    @Test
    void aSharedCaptureTellsItsSourceTheWriteThroughCheckpointItResumesFrom() {
        HazelcastInstance member = emptyDurableMember();
        try {
            InMemoryMeta meta = new InMemoryMeta();
            CaptureRunSpec owner = new CaptureRunSpec(config(), ReadMode.CDC_ONLY, "chain-acknowledged-checkpoint",
                    true, "root_source", "root_pipeline", StartFrom.latest(), null, 0L)
                    .withConsumerId(SrsConsumerId.of("root_pipeline", "root_source").value());
            String chain = owner.miningChainId().value();
            meta.create(chain, null);
            FakeSource first = new FakeSource(List.of(), List.of());
            try (CaptureRun initial = new CaptureRunUnit(first, new SrsCoordinator(meta), meta, member)
                    .start(owner, e -> { })) {
                assertThat(first.acknowledged).as("a fresh chain has no position to release up to").isEmpty();
            }
            SrsMeta record = meta.read(chain).orElseThrow();
            meta.seed(new SrsMeta(chain, new ChainPosition(new SourceOrder(record.epoch(), 3L), "checkpoint-3"),
                    record.consumerOffsets(), record.schemaHistory(), record.retention(), record.epoch(), null,
                    true));

            FakeSource restarted = new FakeSource(List.of(), List.of());
            try (CaptureRun resumed = new CaptureRunUnit(restarted, new SrsCoordinator(meta), meta, member)
                    .start(owner, e -> { })) {
                assertThat(restarted.cdcStart)
                        .isEqualTo(CaptureStart.resume(new SourcePosition("checkpoint-3")));
                assertThat(restarted.acknowledged).containsExactly("checkpoint-3");
            }
        } finally {
            member.shutdown();
        }
    }

    /**
     * A direct channel tells its source the position the channel resumes from, which only ever moves once its
     * targets have confirmed what came before it; closing the run stops that and closes the tail.
     */
    @Test
    void aDirectChannelTellsItsSourceTheCheckpointItResumesFrom() {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureRunSpec spec = spec(ReadMode.CDC_ONLY, false, "chain-direct-acknowledged");
        MiningChainId chainId = spec.miningChainId();
        meta.create(chainId.value(), null);
        meta.advanceSourceReadOffset(chainId.value(), new ChainPosition(new SourceOrder(1L, 7L), "src-11"));

        FakeSource port = new FakeSource(List.of(), List.of());
        try (CaptureRun run = runUnit(port, meta).start(spec, e -> { })) {
            assertThat(port.cdcStart).isEqualTo(CaptureStart.resume(new SourcePosition("src-11")));
            assertThat(port.acknowledged).containsExactly("src-11");
        }
        assertThat(port.cdcClosed).as("closing the run closes the tail it followed").isTrue();
    }

    /**
     * A capture whose rings write through reads through the physical capture's notes, so letting go of what
     * its connector set up is asked of those -- carried over from the node of the pipeline asking, all a
     * cleared chain has left to carry from -- and not of the node's own notes, which such a capture never
     * wrote to.
     */
    @Test
    void aSharedCaptureIsReleasedThroughThePhysicalCapturesNotes() {
        HazelcastInstance member = emptyDurableMember();
        try {
            InMemoryMeta meta = new InMemoryMeta();
            CaptureRunSpec owner = new CaptureRunSpec(config(), ReadMode.CDC_ONLY, "chain-released-shared",
                    true, "root_source", "root_pipeline", StartFrom.latest(), null, 0L)
                    .withConsumerId(SrsConsumerId.of("root_pipeline", "root_source").value());
            FakeSource port = new FakeSource(List.of(), List.of());

            assertThat(new CaptureRunUnit(port, new SrsCoordinator(meta), meta, member).release(owner)).isEmpty();

            PipelineNode node = new PipelineNode("root_pipeline", "root_source");
            assertThat(port.released).singleElement().satisfies(released -> {
                assertThat(released.sharedNotes())
                        .isEqualTo(new SharedNotes(owner.miningChainId().value(), List.of(node)));
                assertThat(released.node()).isEqualTo(node);
            });
        } finally {
            member.shutdown();
        }
    }

    /**
     * A direct channel -- and a chain whose rings do not write through, which only a pipeline reading as its
     * own consumer runs -- reads through its node's own notes, and letting go is asked of those; what the
     * source refuses comes back as it was answered.
     */
    @Test
    void aTailThroughTheNodesOwnNotesIsReleasedThroughThem() {
        FakeSource port = new FakeSource(List.of(), List.of());
        TapstateException refused = new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "no"), null);
        port.refusal = Optional.of(refused);

        assertThat(runUnit(port, new InMemoryMeta()).release(spec(ReadMode.CDC_ONLY, false, "chain-released-direct")))
                .containsSame(refused);
        assertThat(runUnit(port, new InMemoryMeta()).release(spec(ReadMode.CDC_ONLY, true, "chain-released-own")))
                .containsSame(refused);

        assertThat(port.released).hasSize(2).allSatisfy(released -> {
            assertThat(released.sharedNotes()).isNull();
            assertThat(released.node()).isEqualTo(new PipelineNode("pipe-1", "src-1"));
        });
    }

    /** A read with no change tail set nothing up for one, so nothing is asked of its source. */
    @Test
    void aReadWithNoTailReleasesNothing() {
        FakeSource port = new FakeSource(List.of(), List.of());

        assertThat(runUnit(port, new InMemoryMeta()).release(spec(ReadMode.SNAPSHOT_ONLY, true))).isEmpty();
        assertThat(runUnit(port, new InMemoryMeta()).release(spec(ReadMode.SNAPSHOT_ONLY, false))).isEmpty();

        assertThat(port.released).isEmpty();
    }

    /**
     * Closing the unit stops telling its tails' sources how far they may release: the thread that reads for
     * them ends with the unit, rather than outliving the server that closed it.
     */
    @Test
    void closingTheUnitStopsTellingItsSources() throws InterruptedException {
        CaptureRunUnit unit = runUnit(new FakeSource(List.of(), List.of()), new InMemoryMeta());
        assertThat(unit.acknowledging()).isTrue();

        unit.close();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (unit.acknowledging() && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertThat(unit.acknowledging()).as("the unit's reads stopped with it").isFalse();
    }
}
