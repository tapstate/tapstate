package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorNoteOrigin;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.LocalConnectorArtifacts;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.CaptureStatePreparations;
import io.tapstate.spi.store.ConnectorBindingStore;
import io.tapstate.spi.store.ConnectorRunBindings;
import io.tapstate.spi.store.ConnectorRunBindings.Binding;
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
import io.tapstate.spi.store.SrsStoreViews;
import io.tapstate.spi.store.WorkloadClaimFence;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Reserves one exact artifact selection for an explicit start, and publishes it before submitting the run. */
final class PipelineConnectorExecutions {
    record Node(String id, PipelineNode.Purpose purpose, String connectorId, Map<String, Object> settings,
                String canonicalSharedChain) {
        RequiredNoteOwner privateOwner(PipelineIntentScope scope) {
            return RequiredNoteOwner.forNode(scope.bindingVersion(), new PipelineNode(scope.pipelineId(), id),
                    purpose, scope.stateGeneration());
        }
    }

    record Selection(long intentRevision, List<Binding> current, List<Binding> previous) {
        static final Selection EMPTY = new Selection(0, List.of(), List.of());

        Binding binding(String node, PipelineNode.Purpose purpose) {
            return find(current, node, purpose);
        }

        Binding previous(String node, PipelineNode.Purpose purpose) {
            return find(previous, node, purpose);
        }
    }

    private final PipelineIntentStore intents;
    private final CaptureStatePreparations captures;
    private final RequiredNoteStore notes;
    private final SrsStoreViews srs;
    private final ConnectorProvisioner connectors;
    private final java.util.function.Predicate<PipelineIntentScope> recoveryPinned;

    PipelineConnectorExecutions(PipelineIntentStore intents, CaptureStatePreparations captures,
            RequiredNoteStore notes, SrsStoreViews srs, ConnectorProvisioner connectors,
            java.util.function.Predicate<PipelineIntentScope> recoveryPinned) {
        this.intents = intents;
        this.captures = captures;
        this.notes = notes;
        this.srs = srs;
        this.connectors = connectors;
        this.recoveryPinned = recoveryPinned;
    }

    Selection effective(PipelineIntentScope scope, Runnable guard) {
        if (!(connectors instanceof LocalConnectorArtifacts)) return Selection.EMPTY;
        return store(guard).load(scope).map(value -> new Selection(value.effectiveIntentRevision(),
                value.effective(), List.of())).orElse(Selection.EMPTY);
    }

    Binding recoveryBinding(PipelineIntentScope scope, Node source, String sourceHash, String pipelineHash, Runnable guard) {
        if (!(connectors instanceof LocalConnectorArtifacts)) return null;
        var fixed = notes.guardedBy(guard);
        var privateScope = fixed.open(source.privateOwner(scope));
        var required = fixed.origin(privateScope);
        var original = captures.guardedBy(guard).inspectSharedOrigin(business(scope), source.id(), sourceHash, scope.pipelineId(), pipelineHash);
        if (original.isEmpty() || original.orElseThrow().equals(required)) return null;
        var ref = connectors.resolveRetained(fixed, privateScope, required);
        String chain = SourceCaptureIdentity.versionedSharedChain(source.canonicalSharedChain(), ref.contentHash(), null);
        String capture = SourceCaptureIdentity.sharedCapture(chain);
        var existing = captures.guardedBy(guard).read(business(scope), capture).orElse(null);
        var owner = chainOwner(scope, chain);
        return binding(source, ref.contentHash(), existing == null ? new RequiredNoteScope(owner, 1) : fixed.open(owner),
                capture, existing == null ? chain : existing.chainId());
    }

    Selection selection(PipelineIntentScope scope, Runnable guard) {
        if (!(connectors instanceof LocalConnectorArtifacts)) return Selection.EMPTY;
        var port = intents.guardedBy(guard);
        var state = port.connectorBindings().load(scope).orElse(null);
        if (state == null) return Selection.EMPTY;
        var pending = state.handoff();
        var intent = port.load(scope.pipelineId()).orElseThrow(PipelineConnectorExecutions::fenced);
        if (!scope.equals(intent.scope())) throw fenced();
        if (pending != null && pending.phase() == Phase.PREPARED && pending.intentRevision() == intent.intentRevision())
            return new Selection(pending.intentRevision(), pending.candidates(), state.effective());
        return new Selection(state.effectiveIntentRevision(), state.effective(), List.of());
    }

    Map<String, String> retainedSinkHashes(PipelineIntentSnapshot intent, List<Node> nodes, Runnable guard) {
        if (!(connectors instanceof LocalConnectorArtifacts)
                || nodes.stream().allMatch(node -> node.purpose() == PipelineNode.Purpose.CAPTURE)) return Map.of();
        var scope = intent.scope();
        var state = store(guard).load(scope).orElse(null);
        if (state != null && state.handoff() != null && state.handoff().phase() != Phase.PUBLISHED
                && state.handoff().intentRevision() == intent.intentRevision()) return Map.of();

        // An unseeded member must stage the retained sink artifact before it can prove readiness.
        var fixed = notes.guardedBy(guard);
        var hashes = new LinkedHashMap<String, String>();
        for (var node : nodes) {
            if (node.purpose() == PipelineNode.Purpose.CAPTURE) continue;
            var binding = state == null ? null : state.find(node.id(), node.purpose()).orElse(null);
            var token = fixed.open(binding == null ? node.privateOwner(scope) : binding.notes().owner());
            var origin = fixed.originIfPresent(token).orElse(null);
            if (origin == null && binding == null) continue;
            if (origin == null || binding != null && !binding.sha256().equals(origin.artifactHash())
                    || !origin.equals(ConnectorNoteOrigin.capture(node.connectorId(), origin.artifactHash(), node.settings())))
                throw new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
            var ref = connectors.resolveRetained(fixed, token, origin);
            String previous = hashes.putIfAbsent(node.connectorId(), ref.contentHash());
            if (previous != null && !previous.equals(ref.contentHash())) throw fenced();
        }
        return Map.copyOf(hashes);
    }

    void prepare(PipelineIntentSnapshot intent, List<Node> nodes, WorkloadClaimFence claim, Runnable guard) {
        connectors.withArtifactRetention(() -> {
            prepareRetainingArtifacts(intent, nodes, claim, guard);
            return null;
        });
    }

    private void prepareRetainingArtifacts(PipelineIntentSnapshot intent, List<Node> nodes, WorkloadClaimFence claim,
            Runnable guard) {
        var bindings = store(guard);
        var scope = intent.scope();
        var state = bindings.load(scope).orElse(null);
        if (state == null || state.handoff() == null) return;
        var pending = state.handoff();
        if (!(connectors instanceof LocalConnectorArtifacts) || pending.phase() == Phase.PUBLISHED
                || pending.intentRevision() != intent.intentRevision()) {
            settle(scope, state, claim, guard);
            return;
        }
        var fixedNotes = notes.guardedBy(guard);
        if (pending.phase() == Phase.REQUESTED) {
            if (state.effective().isEmpty() && intent.intentRevision() > 1) {
                var previous = new ArrayList<Binding>();
                for (var node : nodes) {
                    var retained = legacy(scope, node, fixedNotes, guard);
                    if (retained != null) previous.add(retained);
                }
                if (!previous.isEmpty()) state = bindings.seed(intent, previous);
            }
            var selected = new ArrayList<Binding>();
            for (var node : nodes) {
                var old = find(state.effective(), node.id(), node.purpose());
                selected.add(select(scope, intent.intentRevision(), node, old, fixedNotes, guard));
            }
            state = bindings.prepare(intent, selected);
        }
        for (var node : nodes) {
            var candidate = find(state.handoff().candidates(), node.id(), node.purpose());
            if (candidate != null && candidate.chainId() == null) {
                var old = find(state.effective(), node.id(), node.purpose());
                stagePrivate(candidate, old, node, fixedNotes);
            }
        }
    }

    void publish(PipelineIntentSnapshot intent, List<Node> nodes, WorkloadClaimFence claim, Runnable guard) {
        if (!(connectors instanceof LocalConnectorArtifacts)) return;
        var bindings = store(guard);
        var state = bindings.load(intent.scope()).orElse(null);
        if (state == null || state.effective().isEmpty() && state.handoff() == null) {
            if (recoveryPinned.test(intent.scope())) return;
            var fixed = notes.guardedBy(guard);
            var observed = new ArrayList<Binding>();
            for (var node : nodes) {
                var retained = legacy(intent.scope(), node, fixed, guard);
                if (retained == null) return;
                observed.add(retained);
            }
            if (!observed.isEmpty()) bindings.seed(intent, observed);
            return;
        }
        if (state.handoff() == null || state.handoff().intentRevision() != intent.intentRevision()) return;
        if (state.handoff().phase() == Phase.PREPARED) state = bindings.publish(intent);
        if (state.handoff().phase() != Phase.PUBLISHED) throw fenced();
        detach(intent.scope(), state.handoff().previous(), state.effective(), claim, guard);
        bindings.complete(intent.scope(), intent.intentRevision());
    }

    void stopped(PipelineIntentSnapshot intent, WorkloadClaimFence claim, Runnable guard) {
        var state = store(guard).load(intent.scope()).orElse(null);
        if (state != null && state.handoff() != null
                && (state.handoff().intentRevision() != intent.intentRevision()
                    || intent.desired().targetState() != io.tapstate.core.lifecycle.PipelineState.RUNNING)) {
            settle(intent.scope(), state, claim, guard);
        }
    }

    private void settle(PipelineIntentScope scope, ConnectorRunBindings state, WorkloadClaimFence claim, Runnable guard) {
        var pending = state.handoff();
        var bindings = store(guard);
        if (pending.phase() == Phase.PUBLISHED) {
            detach(scope, pending.previous(), state.effective(), claim, guard);
            bindings.complete(scope, pending.intentRevision());
        } else {
            detach(scope, pending.candidates(), state.effective(), claim, guard);
            bindings.abort(scope, pending.intentRevision());
        }
    }

    void abandon(PipelineIntentSnapshot intent, WorkloadClaimFence claim, Runnable guard) {
        if (!(connectors instanceof LocalConnectorArtifacts)) return;
        var bindings = store(guard);
        var state = bindings.load(intent.scope()).orElse(null);
        if (state == null || state.handoff() == null || state.handoff().intentRevision() != intent.intentRevision()
                || state.handoff().phase() == Phase.PUBLISHED) return;
        detach(intent.scope(), state.handoff().candidates(), state.effective(), claim, guard);
        bindings.abort(intent.scope(), intent.intentRevision());
    }

    private void detach(PipelineIntentScope scope, List<Binding> from, List<Binding> retained,
            WorkloadClaimFence claim, Runnable guard) {
        if (from.stream().noneMatch(binding -> binding.chainId() != null)) return;
        var consumers = srs.srs(business(scope), guard).consumers();
        for (var old : from) {
            if (old.chainId() == null) continue;
            var replacement = find(retained, old.nodeId(), old.purpose());
            if (replacement != null && old.chainId().equals(replacement.chainId())) continue;
            var context = new SrsConsumerContext(new SrsConsumerKey(old.chainId(), scope.pipelineId(), old.nodeId()),
                    scope.stateGeneration());
            var member = consumers.find(context.key()).orElse(null);
            if (member != null && !member.detached()) {
                if (!context.equals(member.context())
                        || consumers.detach(context, member.revision(), claim).isEmpty()) throw fenced();
            }
        }
    }

    private Binding legacy(PipelineIntentScope scope, Node node, RequiredNoteStore fixed, Runnable guard) {
        String chain = node.canonicalSharedChain();
        if (chain == null) {
            var token = fixed.open(node.privateOwner(scope));
            return fixed.originIfPresent(token).map(origin -> {
                connectors.resolveRetained(fixed, token, origin);
                return binding(node, origin.artifactHash(), token, null, null);
            }).orElse(null);
        }
        var prep = captures.guardedBy(guard).read(business(scope), SourceCaptureIdentity.sharedCapture(chain)).orElse(null);
        if (prep == null) return null;
        var context = new SrsConsumerContext(new SrsConsumerKey(prep.chainId(), scope.pipelineId(), node.id()),
                scope.stateGeneration());
        var member = srs.srs(business(scope), guard).consumers().find(context.key()).orElse(null);
        if (member == null || member.detached() || !context.equals(member.context())) return null;
        var token = fixed.open(chainOwner(scope, prep.canonicalChainId()));
        var origin = fixed.origin(token);
        connectors.resolveRetained(fixed, token, origin);
        return binding(node, origin.artifactHash(), token, prep.captureId(), prep.chainId());
    }

    private Binding select(PipelineIntentScope scope, long revision, Node node, Binding old,
            RequiredNoteStore fixed, Runnable guard) {
        var ref = connectors.resolve(node.connectorId());
        connectors.retain(ref);
        var hash = ref.contentHash();
        if (old != null && old.connectorId().equals(node.connectorId()) && old.sha256().equals(hash)) return old;
        if (node.canonicalSharedChain() == null) {
            var owner = old == null ? node.privateOwner(scope)
                    : RequiredNoteOwner.forArtifact(scope.bindingVersion(), new PipelineNode(scope.pipelineId(), node.id()),
                            node.purpose(), scope.stateGeneration(), hash, revision);
            var token = old == null ? fixed.open(owner) : new RequiredNoteScope(owner, 1);
            return binding(node, hash, token, null, null);
        }
        var canonical = node.canonicalSharedChain();
        var capture = SourceCaptureIdentity.sharedCapture(canonical);
        var oldShared = captures.guardedBy(guard).read(business(scope), capture).orElse(null);
        if (old == null && oldShared == null)
            return binding(node, hash, new RequiredNoteScope(chainOwner(scope, canonical), 1), capture, canonical);
        if (old == null) {
            var token = fixed.open(chainOwner(scope, oldShared.canonicalChainId()));
            if (fixed.originIfPresent(token).filter(origin -> origin.artifactHash().equals(hash)).isPresent())
                return binding(node, hash, token, capture, oldShared.chainId());
        }
        String discriminator = old == null ? null : discriminator(scope, revision);
        var chain = SourceCaptureIdentity.versionedSharedChain(canonical, hash, discriminator);
        capture = SourceCaptureIdentity.sharedCapture(chain);
        var prepared = captures.guardedBy(guard).read(business(scope), capture).orElse(null);
        var noteOwner = chainOwner(scope, chain);
        return binding(node, hash, prepared == null ? new RequiredNoteScope(noteOwner, 1) : fixed.open(noteOwner),
                capture, prepared == null ? chain : prepared.chainId());
    }

    private void stagePrivate(Binding candidate, Binding old, Node node, RequiredNoteStore fixed) {
        var origin = ConnectorNoteOrigin.capture(candidate.connectorId(), candidate.sha256(), node.settings());
        if (old == null || old.notes().equals(candidate.notes())) {
            if (!fixed.prepareOrigin(candidate.notes(), origin).equals(origin)) throw fenced();
            connectors.markBound(fixed, candidate.notes(), origin);
        } else {
            var token = fixed.prepareArtifactBinding(candidate.notes().owner(), origin, old.notes());
            if (!token.equals(candidate.notes())) throw fenced();
        }
    }

    private Binding binding(Node node, String sha256, RequiredNoteScope token, String capture, String chain) {
        return new Binding(node.id(), node.purpose(), node.connectorId(),
                connectors.artifactVersion(node.connectorId(), sha256).orElseThrow(PipelineConnectorExecutions::fenced),
                sha256, token, capture, chain);
    }

    static RequiredNoteOwner chainOwner(PipelineIntentScope scope, String chain) {
        return new RequiredNoteOwner(scope.bindingVersion(), "pdk.chain." + chain, RequiredNoteOwner.Kind.CHAIN, chain, 0);
    }

    static String discriminator(PipelineIntentScope scope, long revision) {
        return scope.pipelineId() + ":" + scope.stateGeneration() + ":" + revision;
    }

    static CacheBusinessScope business(PipelineIntentScope scope) {
        return new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation());
    }

    private ConnectorBindingStore store(Runnable guard) {
        return intents.guardedBy(guard).connectorBindings();
    }

    private static Binding find(List<Binding> bindings, String node, PipelineNode.Purpose purpose) {
        return bindings.stream().filter(binding -> binding.nodeId().equals(node) && binding.purpose() == purpose)
                .findFirst().orElse(null);
    }

    private static TapstateException fenced() {
        return new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
    }
}
