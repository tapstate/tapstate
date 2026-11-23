package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorNoteOrigin;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.LocalConnectorArtifacts;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import io.tapstate.spi.store.CapturePreparation;
import io.tapstate.spi.store.CaptureStatePreparations;
import io.tapstate.spi.store.ConnectorBindingStore;
import io.tapstate.spi.store.ConnectorRunBindings;
import io.tapstate.spi.store.ConnectorRunBindings.Binding;
import io.tapstate.spi.store.ConnectorRunBindings.Handoff;
import io.tapstate.spi.store.ConnectorRunBindings.Phase;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.PipelineIntentStore;
import io.tapstate.spi.store.RequiredNoteOwner;
import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.RequiredNoteStore;
import io.tapstate.spi.store.SrsConsumerContext;
import io.tapstate.spi.store.SrsConsumerKey;
import io.tapstate.spi.store.SrsConsumerMembership;
import io.tapstate.spi.store.SrsConsumerStore;
import io.tapstate.spi.store.SrsStoreViews;
import io.tapstate.spi.store.SrsStores;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class PipelineConnectorExecutionsTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 1, "cache", "flow", 7);
    private static final PipelineIntentSnapshot START = intent(SCOPE, 2, PipelineState.RUNNING);
    private static final PipelineIntentSnapshot STOPPED = intent(SCOPE, 3, PipelineState.STOPPED);
    private static final String CONNECTOR = "mongodb";
    private static final String OLD_HASH = "a".repeat(64);
    private static final String NEW_HASH = "b".repeat(64);
    private static final String CHAIN = "mck-orders";
    private static final WorkloadClaimFence CLAIM = new WorkloadClaimFence(
            new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "flow"),
            new WorkloadOwner("node", "boot"), 3, 2, 1);
    private static final Runnable ACTIVE = () -> {};
    private static final PipelineConnectorExecutions.Node SOURCE = new PipelineConnectorExecutions.Node(
            "input", PipelineNode.Purpose.CAPTURE, CONNECTOR, Map.of("database", "orders"), CHAIN);
    private static final PipelineConnectorExecutions.Node TARGET = new PipelineConnectorExecutions.Node(
            "target", PipelineNode.Purpose.SYNC, CONNECTOR, Map.of("database", "output"), null);

    @Test
    void stagesPrivateNotesThenPublishesBindingsBeforeDetachingTheOriginalSharedMember() {
        var f = new Fixture();
        var original = List.of(shared(SOURCE, CHAIN, OLD_HASH), privateBinding(TARGET, OLD_HASH));
        var requested = new Handoff(2, Phase.REQUESTED, List.of(), List.of());
        when(f.bindings.load(SCOPE)).thenReturn(Optional.of(state(1, original, requested)));
        var handoffChain = SourceCaptureIdentity.versionedSharedChain(CHAIN, NEW_HASH,
                PipelineConnectorExecutions.discriminator(SCOPE, 2));
        var newNotes = new RequiredNoteScope(RequiredNoteOwner.forArtifact(1, new PipelineNode("flow", TARGET.id()),
                TARGET.purpose(), 7, NEW_HASH, 2), 1);
        var candidate = List.of(shared(SOURCE, handoffChain, NEW_HASH),
                new Binding(TARGET.id(), TARGET.purpose(), CONNECTOR, "2.0", NEW_HASH, newNotes, null, null));
        var pending = new Handoff(2, Phase.PREPARED, candidate, List.of());
        when(f.bindings.prepare(START, candidate)).thenReturn(state(1, original, pending));
        when(f.notes.prepareArtifactBinding(newNotes.owner(), origin(TARGET, NEW_HASH), original.getLast().notes()))
                .thenReturn(newNotes);

        f.executions.prepare(START, List.of(SOURCE, TARGET), CLAIM, ACTIVE);
        verify(f.connectors, times(2)).retain(ref(NEW_HASH));
        verify(f.notes).prepareArtifactBinding(newNotes.owner(), origin(TARGET, NEW_HASH), original.getLast().notes());
        verifyNoInteractions(f.consumers);

        when(f.bindings.load(SCOPE)).thenReturn(Optional.of(state(1, original, pending)));
        when(f.intents.load("flow")).thenReturn(Optional.of(START));
        var chosen = f.executions.selection(SCOPE, ACTIVE);
        assertThat(chosen.binding(SOURCE.id(), SOURCE.purpose())).isEqualTo(candidate.getFirst());
        assertThat(chosen.previous(TARGET.id(), TARGET.purpose())).isEqualTo(original.getLast());
        assertThat(f.executions.effective(SCOPE, ACTIVE).current()).containsExactlyElementsOf(original);

        var detached = f.member(CHAIN);
        var published = new Handoff(2, Phase.PUBLISHED, candidate, original);
        when(f.bindings.publish(START)).thenReturn(state(2, candidate, published));
        f.executions.publish(START, List.of(SOURCE, TARGET), CLAIM, ACTIVE);
        var order = inOrder(f.bindings, f.consumers);
        order.verify(f.bindings).publish(START);
        order.verify(f.consumers).detach(detached.context(), detached.revision(), CLAIM);
        order.verify(f.bindings).complete(SCOPE, 2);
    }

    @Test
    void abandoningAPreparedHandoffDetachesOnlyItsCandidateAndKeepsTheEffectiveBinding() {
        var f = new Fixture();
        var effective = List.of(shared(SOURCE, CHAIN, OLD_HASH));
        var staged = List.of(shared(SOURCE, "mcv-candidate", NEW_HASH));
        var prepared = state(1, effective, new Handoff(2, Phase.PREPARED, staged, List.of()));
        when(f.bindings.load(SCOPE)).thenReturn(Optional.of(prepared));
        var candidate = f.member("mcv-candidate");

        f.executions.abandon(START, CLAIM, ACTIVE);
        verify(f.consumers).detach(candidate.context(), candidate.revision(), CLAIM);
        verify(f.consumers, never()).find(key(CHAIN));
        verify(f.bindings).abort(SCOPE, 2);
        verify(f.bindings, never()).publish(any());
    }

    @Test
    void stoppingAfterPublicationFinishesTheOldHandoffUsingItsRevision() {
        var f = new Fixture();
        var old = List.of(shared(SOURCE, CHAIN, OLD_HASH));
        var effective = List.of(shared(SOURCE, "mcv-candidate", NEW_HASH));
        var published = state(2, effective, new Handoff(2, Phase.PUBLISHED, effective, old));
        when(f.bindings.load(SCOPE)).thenReturn(Optional.of(published));
        var retired = f.member(CHAIN);
        f.executions.stopped(STOPPED, CLAIM, ACTIVE);
        verify(f.consumers).detach(retired.context(), retired.revision(), CLAIM);
        verify(f.bindings).complete(SCOPE, 2);
        verify(f.bindings, never()).abort(any(), anyLong());
    }

    @Test
    void retainedSinkReadinessUsesTheOriginalHashAndRejectsAChangedOrigin() {
        var f = new Fixture();
        var retained = privateBinding(TARGET, OLD_HASH);
        when(f.bindings.load(SCOPE)).thenReturn(Optional.of(state(1, List.of(retained), null)));
        var originalOrigin = origin(TARGET, OLD_HASH);
        when(f.notes.originIfPresent(retained.notes())).thenReturn(Optional.of(originalOrigin));
        when(f.connectors.resolveRetained(f.notes, retained.notes(), originalOrigin)).thenReturn(ref(OLD_HASH));
        assertThat(f.executions.retainedSinkHashes(START, List.of(TARGET), ACTIVE)).containsEntry(CONNECTOR, OLD_HASH);
        verify(f.connectors).resolveRetained(f.notes, retained.notes(), originalOrigin);
        verify(f.connectors, never()).resolve(CONNECTOR);

        when(f.notes.originIfPresent(retained.notes())).thenReturn(Optional.of(origin(TARGET, NEW_HASH)));
        assertThatThrownBy(() -> f.executions.retainedSinkHashes(START, List.of(TARGET), ACTIVE))
                .isInstanceOfSatisfying(TapstateException.class,
                        refused -> assertThat(refused.code()).isEqualTo(IoError.NOTE_SCOPE_UNAVAILABLE));
    }

    @Test
    void initiallyPublishedPrivateBindingKeepsTheObservedArtifactWithoutSelectingLatest() {
        var f = new Fixture();
        var original = privateBinding(TARGET, OLD_HASH);
        when(f.bindings.load(SCOPE)).thenReturn(Optional.empty());
        when(f.notes.originIfPresent(original.notes())).thenReturn(Optional.of(origin(TARGET, OLD_HASH)));

        f.executions.publish(START, List.of(TARGET), CLAIM, ACTIVE);
        verify(f.bindings).seed(START, List.of(original));
        verify(f.connectors).resolveRetained(f.notes, original.notes(), origin(TARGET, OLD_HASH));
        verify(f.connectors, never()).resolve(CONNECTOR);
    }

    @Test
    void recoveryUsesASeparateChainForTheRetainedPrivateHash() {
        var f = new Fixture();
        var scope = PipelineConnectorExecutions.business(SCOPE);
        var privateNotes = new RequiredNoteScope(SOURCE.privateOwner(SCOPE), 1);
        var retained = origin(SOURCE, OLD_HASH);
        when(f.notes.origin(privateNotes)).thenReturn(retained);
        when(f.capture.inspectSharedOrigin(scope, SOURCE.id(), "source-hash", SCOPE.pipelineId(), "pipeline-hash"))
                .thenReturn(Optional.of(retained));
        assertThat(f.executions.recoveryBinding(SCOPE, SOURCE, "source-hash", "pipeline-hash", ACTIVE)).isNull();
        verify(f.connectors, never()).resolveRetained(any(), any(), any());

        when(f.capture.inspectSharedOrigin(scope, SOURCE.id(), "source-hash", SCOPE.pipelineId(), "pipeline-hash"))
                .thenReturn(Optional.of(origin(SOURCE, NEW_HASH)));
        when(f.connectors.resolveRetained(f.notes, privateNotes, retained)).thenReturn(ref(OLD_HASH));
        String retainedChain = SourceCaptureIdentity.versionedSharedChain(CHAIN, OLD_HASH, null);
        var expected = shared(SOURCE, retainedChain, OLD_HASH);
        assertThat(f.executions.recoveryBinding(SCOPE, SOURCE, "source-hash", "pipeline-hash", ACTIVE))
                .isEqualTo(expected);
        verify(f.connectors, never()).resolve(CONNECTOR);
    }

    @Test
    void firstSharedStartJoinsOnlyAChainBoundToItsSelectedArtifact() {
        var f = new Fixture();
        var scope = PipelineConnectorExecutions.business(SCOPE);
        var olderChain = shared(SOURCE, CHAIN, OLD_HASH);
        var prep = new CapturePreparation(scope, CapturePreparation.Mode.SHARED_SRS,
                CHAIN, olderChain.captureId(), "mc-" + OLD_HASH, UUID.randomUUID().toString());
        when(f.capture.read(scope, olderChain.captureId())).thenReturn(Optional.of(prep));
        var first = intent(SCOPE, 1, PipelineState.RUNNING);
        var requested = state(0, List.of(), new Handoff(1, Phase.REQUESTED, List.of(), List.of()));
        when(f.bindings.load(SCOPE)).thenReturn(Optional.of(requested));
        when(f.bindings.prepare(eq(first), any())).thenAnswer(call -> state(0, List.of(),
                new Handoff(1, Phase.PREPARED, call.getArgument(1), List.of())));

        when(f.notes.originIfPresent(olderChain.notes())).thenReturn(Optional.of(origin(SOURCE, OLD_HASH)));
        f.executions.prepare(first, List.of(SOURCE), CLAIM, ACTIVE);
        var independent = shared(SOURCE, SourceCaptureIdentity.versionedSharedChain(CHAIN, NEW_HASH, null), NEW_HASH);
        verify(f.bindings).prepare(first, List.of(independent));

        when(f.notes.originIfPresent(olderChain.notes())).thenReturn(Optional.of(origin(SOURCE, NEW_HASH)));
        f.executions.prepare(first, List.of(SOURCE), CLAIM, ACTIVE);
        verify(f.bindings).prepare(first, List.of(shared(SOURCE, CHAIN, NEW_HASH)));
    }

    @Test
    void restartingWithTheSameHashKeepsItsExistingPrivateNoteOwner() {
        var f = new Fixture();
        var oldBinding = privateBinding(TARGET, NEW_HASH);
        var current = List.of(oldBinding);
        var origin = origin(TARGET, NEW_HASH);
        when(f.bindings.load(SCOPE)).thenReturn(Optional.of(state(1, current,
                new Handoff(2, Phase.REQUESTED, List.of(), List.of()))));
        when(f.bindings.prepare(START, current)).thenReturn(state(1, current,
                new Handoff(2, Phase.PREPARED, current, List.of())));
        when(f.notes.prepareOrigin(oldBinding.notes(), origin)).thenReturn(origin);

        f.executions.prepare(START, List.of(TARGET), CLAIM, ACTIVE);

        verify(f.bindings).prepare(START, current);
        verify(f.connectors).markBound(f.notes, oldBinding.notes(), origin);
        verify(f.notes, never()).prepareArtifactBinding(any(), any(), any());
    }

    private static final class Fixture {
        final PipelineIntentStore intents = mock(PipelineIntentStore.class);
        final ConnectorBindingStore bindings = mock(ConnectorBindingStore.class);
        final CaptureStatePreparations capture = mock(CaptureStatePreparations.class);
        final RequiredNoteStore notes = mock(RequiredNoteStore.class);
        final LocalConnectorArtifacts connectors = mock(LocalConnectorArtifacts.class);
        final SrsConsumerStore consumers = mock(SrsConsumerStore.class);
        final PipelineConnectorExecutions executions;

        Fixture() {
            when(intents.guardedBy(ACTIVE)).thenReturn(intents);
            when(intents.connectorBindings()).thenReturn(bindings);
            when(notes.guardedBy(ACTIVE)).thenReturn(notes);
            when(notes.open(any())).thenAnswer(call -> new RequiredNoteScope(call.getArgument(0), 1));
            when(connectors.withArtifactRetention(any())).thenCallRealMethod();
            when(connectors.resolve(CONNECTOR)).thenReturn(ref(NEW_HASH));
            when(connectors.artifactVersion(CONNECTOR, NEW_HASH)).thenReturn(Optional.of("2.0"));
            when(connectors.artifactVersion(CONNECTOR, OLD_HASH)).thenReturn(Optional.of("1.0"));
            when(capture.guardedBy(ACTIVE)).thenReturn(capture);
            var srs = mock(SrsStoreViews.class);
            var stores = mock(SrsStores.class);
            when(stores.consumers()).thenReturn(consumers);
            when(srs.srs(PipelineConnectorExecutions.business(SCOPE), ACTIVE)).thenReturn(stores);
            executions = new PipelineConnectorExecutions(intents, capture, notes, srs, connectors, scope -> false);
        }

        SrsConsumerMembership member(String chain) {
            var context = new SrsConsumerContext(key(chain), SCOPE.stateGeneration());
            var membership = new SrsConsumerMembership(context, 4, false, Set.of("orders"), CLAIM);
            when(consumers.find(context.key())).thenReturn(Optional.of(membership));
            when(consumers.detach(context, membership.revision(), CLAIM)).thenReturn(Optional.of(membership));
            return membership;
        }
    }

    private static ConnectorRunBindings state(long revision, List<Binding> effective, Handoff handoff) {
        return new ConnectorRunBindings(SCOPE, revision, effective, handoff);
    }

    private static Binding shared(PipelineConnectorExecutions.Node node, String chain, String hash) {
        return new Binding(node.id(), node.purpose(), CONNECTOR, OLD_HASH.equals(hash) ? "1.0" : "2.0", hash,
                new RequiredNoteScope(PipelineConnectorExecutions.chainOwner(SCOPE, chain), 1),
                SourceCaptureIdentity.sharedCapture(chain), chain);
    }

    private static Binding privateBinding(PipelineConnectorExecutions.Node node, String hash) {
        return new Binding(node.id(), node.purpose(), CONNECTOR, OLD_HASH.equals(hash) ? "1.0" : "2.0", hash,
                new RequiredNoteScope(node.privateOwner(SCOPE), 1), null, null);
    }

    private static SrsConsumerKey key(String chain) {
        return new SrsConsumerKey(chain, SCOPE.pipelineId(), SOURCE.id());
    }

    private static io.tapstate.spi.store.RequiredNoteOrigin origin(PipelineConnectorExecutions.Node node, String hash) {
        return ConnectorNoteOrigin.capture(CONNECTOR, hash, node.settings());
    }

    private static ConnectorRef ref(String hash) {
        return new ConnectorRef(List.of(), "unused", "2.0.11", null, null, hash, false);
    }

    private static PipelineIntentSnapshot intent(PipelineIntentScope scope, long revision, PipelineState targetState) {
        return new PipelineIntentSnapshot(scope, revision, new DesiredState(scope.pipelineId(), targetState, "pinned"));
    }
}
