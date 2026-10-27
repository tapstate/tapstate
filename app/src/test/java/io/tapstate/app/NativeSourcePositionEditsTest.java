package io.tapstate.app;

import io.tapstate.adapters.mongostore.*;
import io.tapstate.control.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.*;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;
import java.time.*;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class NativeSourcePositionEditsTest {
    @Test void failedMutationAttemptsEveryOriginalCaptureReleaseAndKeepsItsPrimaryFailure() {
        var f = new Fixture(); var original = new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
        var cleanup = new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
        var command = f.edits.prepare("flow", f.current, f.moves);
        doThrow(original).when(f.writes).rewrite(any(), any(), any());
        doThrow(cleanup).when(f.captures).release(f.first);
        assertThatThrownBy(command::run).isSameAs(original).satisfies(error -> assertThat(error.getSuppressed()).contains(cleanup));
        verify(f.captures).release(f.second); verify(f.control).close();
    }
    @Test void aCommandDelayedByAuditRefusesChangedConfigurationBeforeAcquiringAnyOwner() {
        var f = new Fixture(); var command = f.edits.prepare("flow", f.current, f.moves);
        when(f.configuration.configurationRevision()).thenReturn(2L);
        assertThatThrownBy(command::run).isInstanceOf(TapstateException.class);
        verifyNoInteractions(f.owners); verifyNoInteractions(f.writes); verifyNoInteractions(f.captures);
    }
    @Test void aDelayedCommandCannotAdoptANewGenerationEvenWithAnUnchangedDefinitionAndRevision() {
        var f = new Fixture(); var command = f.edits.prepare("flow", f.current, f.moves);
        when(f.port.scope("flow")).thenReturn(new PipelineIntentScope("cluster",1,"cache","flow",2));
        assertThatThrownBy(command::run).isInstanceOf(TapstateException.class);
        verifyNoInteractions(f.owners); verifyNoInteractions(f.writes); verifyNoInteractions(f.captures);
    }
    private static final class Fixture {
        final MongoConfigurationDomain configuration = mock(MongoConfigurationDomain.class);
        final ThreeLayerStorePort port = mock(ThreeLayerStorePort.class);
        final PipelineActuationOwnership owners = mock(PipelineActuationOwnership.class);
        final PipelineActuationOwnership.Control control = mock(PipelineActuationOwnership.Control.class);
        final CaptureOwnership captures = mock(CaptureOwnership.class);
        final SourcePositionEdits writes = mock(SourcePositionEdits.class);
        final WorkloadClaim first, second;
        final PipelinePosition current;
        final Map<String,String> moves;
        final NativeSourcePositionEdits edits;
        Fixture() {
            var business = new CacheBusinessScope("cluster", 1, "cache"); var scope = new PipelineIntentScope("cluster", 1, "cache", "flow", 1);
            var cache = mock(MongoCacheDomain.class);
            var reading = mock(PipelinePositionService.class); var artifacts = mock(ArtifactStore.class);
            var sourceA = new SourceResource("source-a", null, "fixture", Map.of("database", "a"), null, null, null, null);
            var sourceB = new SourceResource("source-b", null, "fixture", Map.of("database", "b"), null, null, null, null);
            var pipeline = new PipelineResource("flow", null, List.of(SourceRef.spec("source-a", true), SourceRef.spec("source-b", true)), null, null, null, null, null);
            String a = SourceCaptureIdentity.resolve("fixture", sourceA.config(), null), b = SourceCaptureIdentity.resolve("fixture", sourceB.config(), null);
            var captureNames = new ArrayList<>(List.of(SourceCaptureIdentity.sharedCapture(a), SourceCaptureIdentity.sharedCapture(b))); Collections.sort(captureNames);
            first = capture(captureNames.getFirst()); second = capture(captureNames.getLast());
            current = new PipelinePosition("flow", List.of(new PipelinePosition.Chain(a, "source-a", List.of("orders"), null, null, null, List.of()),
                    new PipelinePosition.Chain(b, "source-b", List.of("orders"), null, null, null, List.of())));
            moves = new LinkedHashMap<>(); moves.put(a, "requested-a"); moves.put(b, "requested-b");
            when(configuration.configurationRevision()).thenReturn(1L); when(configuration.artifacts()).thenReturn(artifacts);
            when(artifacts.list()).thenReturn(List.of(pipeline, sourceA, sourceB));
            when(artifacts.get("flow")).thenReturn(Optional.of(pipeline)); when(artifacts.get("source-a")).thenReturn(Optional.of(sourceA)); when(artifacts.get("source-b")).thenReturn(Optional.of(sourceB));
            var preparations = mock(PipelineStatePreparations.class); when(configuration.preparations()).thenReturn(preparations);
            when(preparations.read("flow")).thenReturn(Optional.of(new PipelinePreparation("flow", 1, PipelinePreparation.Phase.UNASSIGNED, null, null, null)));
            when(port.scope("flow")).thenReturn(scope); when(port.state()).thenReturn(mock(StateStore.class)); when(port.desired()).thenReturn(mock(DesiredStore.class));
            when(reading.read("flow")).thenReturn(current); when(owners.unpreparedControl(scope)).thenReturn(control);
            var chains = mock(SrsChainStore.class); var consumers = mock(SrsConsumerStore.class);
            when(cache.srs(any(), any())).thenReturn(new SrsStores(chains, mock(SrsModelStore.class), mock(SrsLogStore.class), consumers));
            for (String chain : List.of(a, b)) {
                var checkpoint = new SrsCaptureCheckpoint(new ChainPosition(SourceOrder.snapshotRow(1), "old"),
                        Map.of("orders", new SrsTableCheckpoint(new SrsModelKey(chain, "orders", "cache", 1, 0), -1)));
                when(chains.load(chain)).thenReturn(Optional.of(new SrsChainState(chain, SourceCaptureIdentity.sharedCapture(chain), null, 1, Set.of("orders"), Set.of("orders"), 1, checkpoint, Instant.EPOCH, 0, 0)));
                when(consumers.attached(chain)).thenReturn(new SrsConsumerTopology(business, chain, 0, List.of()));
            }
            when(captures.acquire(new CaptureId(first.key().resourceId()))).thenReturn(new CaptureOwnership.Permit(true, first));
            when(captures.acquire(new CaptureId(second.key().resourceId()))).thenReturn(new CaptureOwnership.Permit(true, second));
            when(cache.sourcePositions(any(), any(), any())).thenReturn(writes);
            var node = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, "node"), new WorkloadOwner("node", "boot"), 1, 0, 1);
            edits = new NativeSourcePositionEdits(configuration, cache, business, port, reading, owners, captures, () -> node, () -> {},
                    mock(Engine.class), mock(EngineLifecycleActuator.class), mock(HazelcastOperatorReadiness.class), mock(HazelcastExecutionPlans.class), Clock.systemUTC());
        }
        private static WorkloadClaim capture(String id) { return new WorkloadClaim(new WorkloadClaimKey("cluster", WorkloadClaimType.CAPTURE, id), new WorkloadOwner("node", "boot"), 1, 0, 1, Instant.now().plusSeconds(60)); }
    }
}
