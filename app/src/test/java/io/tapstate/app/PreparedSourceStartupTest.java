package io.tapstate.app;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.ReadMode;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.spi.store.CaptureReadAttempt;
import io.tapstate.spi.store.CaptureReadState;
import io.tapstate.spi.store.CaptureResumePreparation;
import io.tapstate.spi.store.CaptureResumeWitness;
import io.tapstate.spi.store.CaptureStartupProof;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PreparedSourceStartupTest {
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final WorkloadClaimFence PIPELINE = fence(WorkloadClaimType.PIPELINE_ACTUATION, "pipeline");

    @Test
    void aControllerWithNoLocalCaptureLoadsTheOriginalPreparedFacts() {
        SrsMetaStore meta = mock(SrsMetaStore.class);
        var first = prepared("first");
        var second = prepared("second");
        when(meta.captureResumePreparations(PIPELINE)).thenReturn(List.of(first, second));
        when(meta.captureStartupProof(PIPELINE, first.witness())).thenReturn(Optional.of(proof(first)));
        when(meta.captureStartupProof(PIPELINE, second.witness())).thenReturn(Optional.of(proof(second)));
        var coordinator = coordinator(meta);
        assertThat(coordinator.isCapturing("pipeline")).isFalse();
        assertThat(coordinator.startupProofs("pipeline", PIPELINE)).containsOnlyKeys("first", "second");
    }

    @Test
    void aCrashDuringPreparationCannotPublishAPartialSourceSetAsComplete() {
        SrsMetaStore meta = mock(SrsMetaStore.class);
        var onlyPrepared = prepared("first");
        when(meta.captureResumePreparations(PIPELINE)).thenReturn(List.of(onlyPrepared));
        when(meta.captureStartupProof(PIPELINE, onlyPrepared.witness())).thenReturn(Optional.of(proof(onlyPrepared)));
        var coordinator = coordinator(meta);
        assertThat(coordinator.requiredSources("pipeline", PIPELINE)).containsExactlyInAnyOrder("first", "second");
        assertThat(coordinator.startupProofs("pipeline", PIPELINE)).isEmpty();
    }

    @Test
    void everyRequiredSourceMustHaveItsOwnAcceptedReceipt() {
        SrsMetaStore meta = mock(SrsMetaStore.class);
        var first = prepared("first");
        var second = prepared("second");
        when(meta.captureResumePreparations(PIPELINE)).thenReturn(List.of(first, second));
        when(meta.captureStartupProof(PIPELINE, first.witness())).thenReturn(Optional.of(proof(first)));
        when(meta.captureStartupProof(PIPELINE, second.witness())).thenReturn(Optional.empty());
        assertThat(coordinator(meta).startupProofs("pipeline", PIPELINE)).isEmpty();
    }

    @Test
    void aReplacementControllerSurfacesTheQualifiedRemoteFailureWithoutReplacingItsCodeOrPoint() {
        SrsMetaStore meta = mock(SrsMetaStore.class);
        var prepared = prepared("first");
        when(meta.captureResumePreparations(PIPELINE)).thenReturn(List.of(prepared));
        var attempt = proof(prepared).readerState().attempt();
        var failed = new CaptureReadState(attempt, "confirmed", START, null, true,
                "capture.start-from-outside-window", Map.of("requested", "old", "earliest", "head", "retention", "2h"),
                "stop-and-clear-source-state", START.plusSeconds(2));
        var actual = new io.tapstate.spi.store.CaptureStartupFailure(PIPELINE, prepared.witness(), prepared.requestedPosition(), START, failed);
        when(meta.captureStartupFailure(PIPELINE, prepared.witness())).thenReturn(Optional.of(actual));
        var coordinator = coordinator(meta);
        assertThat(coordinator.startupFailures("pipeline", PIPELINE)).containsEntry("first", actual);
        assertThat(coordinator.captureFailure("pipeline", PIPELINE)).get()
                .isInstanceOfSatisfying(io.tapstate.runtime.srs.CaptureStartupException.class, error -> {
                    assertThat(error.failure()).isEqualTo(actual);
                    assertThat(error.code()).isEqualTo(io.tapstate.runtime.srs.CaptureError.READER_STARTUP_FAILED);
                });
        assertThat(actual.requestedPosition().position().token()).isEqualTo("confirmed");
    }

    private static CaptureResumePreparation prepared(String id) {
        CaptureResumeWitness witness = new CaptureResumeWitness(id, "mongo", "chain-" + id, "consumer-" + id,
                ReadMode.CDC_ONLY, false, List.of("orders"), true, 2,
                new ChainPosition(new SourceOrder(2, 9), "confirmed"), false, true,
                List.of(), null, 0, io.tapstate.spi.store.ConsumerProgressKind.DIRECT_SOURCE,
                new ChainPosition(new SourceOrder(2, 9), "confirmed"), Map.of());
        var requested = new io.tapstate.spi.store.ClusterRecoveryPosition(id, "mongo", "capture-" + id,
                io.tapstate.spi.store.ClusterRecoveryPosition.Kind.DURABLE_POSITION,
                new ChainPosition(new SourceOrder(2, 9), "confirmed"), "confirmed-consumer", witness.consumerId());
        return new CaptureResumePreparation(PIPELINE, witness, requested, START, Set.of("first", "second"));
    }

    private static CaptureStartupProof proof(CaptureResumePreparation prepared) {
        CaptureReadAttempt attempt = new CaptureReadAttempt(prepared.witness().miningChainId(), 2, 3,
                fence(WorkloadClaimType.CAPTURE, "capture-" + prepared.witness().sourceId()), List.of("orders"),
                CaptureReadAttempt.Kind.RESUME, "confirmed", null, START);
        CaptureReadState accepted = new CaptureReadState(attempt, "confirmed", START, START.plusSeconds(1), false, null);
        return new CaptureStartupProof(PIPELINE, prepared.witness(), prepared.requestedPosition(), START,
                START.plusSeconds(1), accepted);
    }

    private static WorkloadClaimFence fence(WorkloadClaimType type, String resource) {
        return new WorkloadClaimFence(new WorkloadClaimKey("east", type, resource), new WorkloadOwner("node", "boot"),
                1, type == WorkloadClaimType.CAPTURE ? 0 : 2, 1, 1);
    }

    private static StoreBackedPipelineCaptureCoordinator coordinator(SrsMetaStore meta) {
        StorePort port = mock(StorePort.class);
        when(port.meta()).thenReturn(meta);
        CaptureStarter starter = (spec, handoff) -> { throw new AssertionError("no source is reopened to read startup evidence"); };
        return new StoreBackedPipelineCaptureCoordinator(port, starter, mock(SrsCoordinator.class), new SnapshotBuffer(16));
    }
}
