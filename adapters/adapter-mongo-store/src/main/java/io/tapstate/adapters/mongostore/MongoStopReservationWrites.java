package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.SuccessorAdmission;
import io.tapstate.spi.store.SuccessorEnd;
import io.tapstate.spi.store.HandoffIdentity;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.Document;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** Short checkpoint transactions that also write the desired and authority documents they prove. */
final class MongoStopReservationWrites {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readPreference(ReadPreference.primary())
            .readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final FindOneAndUpdateOptions RETURN_AFTER =
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);
    private static final Document PROVE_DESIRED = new Document("$inc", new Document("stopGuardVersion", 1L));
    private static final Document PROVE_CLAIM = new Document("$inc", new Document("fencedAppends", 1L));
    private static final Fenced FENCED = new Fenced();

    private final MongoClient client;
    private final MongoCollection<Document> states, desired, claims, artifacts;
    private final MongoExecutionGenerationWrites generations;

    MongoStopReservationWrites(MongoClient client, MongoCollection<Document> states,
            MongoCollection<Document> desired, MongoCollection<Document> claims) {
        this(client, states, desired, claims, null);
    }

    MongoStopReservationWrites(MongoClient client, MongoCollection<Document> states,
            MongoCollection<Document> desired, MongoCollection<Document> claims, MongoCollection<Document> artifacts) {
        this.client = Objects.requireNonNull(client, "client");
        this.states = Objects.requireNonNull(states, "states");
        this.desired = Objects.requireNonNull(desired, "desired");
        this.claims = Objects.requireNonNull(claims, "claims");
        this.artifacts = artifacts;
        this.generations = new MongoExecutionGenerationWrites(claims);
    }

    Optional<StopReservation> reserve(CheckpointDoc expected, StopReservation proposal, Instant at) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(proposal, "proposal");
        Objects.requireNonNull(at, "at");
        if (!expected.pipelineId().equals(proposal.pipelineId())
                || proposal.sourceEpoch() != expected.epoch()
                || proposal.reservedEpoch() != Math.addExact(expected.epoch(), 1)) {
            throw new IllegalArgumentException("reservation must name the checkpoint's next epoch");
        }
        if (!proposal.legacy() && proposal.counterPolicy() != StopReservation.CounterPolicy.freeze(expected,
                proposal.originalDesired())) {
            throw new IllegalArgumentException("reservation policy requires the exact actual state proof");
        }
        Document marker = StopReservationDocument.write(proposal);
        return transact(proposal.pipelineId(), session -> {
            Document current = state(session, proposal.pipelineId());
            if (current == null || current.containsKey(StopReservationDocument.FIELD)
                    || !expected.equals(MongoStateStore.toCheckpoint(current))) { throw FENCED; }
            guardDesired(session, proposal.pipelineId(), proposal.originalDesired());
            guardAuthority(session, proposal.pipelineId(), proposal.source(), proposal.writerAuthority());
            if (!proposal.legacy() && proposal.source().scope() != null) {
                guardArtifact(session, proposal.pipelineId(), proposal.source().scope().pipelineIncarnationId());
            }
            Document filter = new Document("_id", proposal.pipelineId()).append("epoch", expected.epoch())
                    .append("stateJson", expected.stateJson())
                    .append(StopReservationDocument.FIELD, new Document("$exists", false));
            Document update = new Document("$set", new Document(StopReservationDocument.FIELD, marker)
                    .append("touchMillis", at.toEpochMilli()))
                    .append("$inc", new Document("epoch", 1L));
            Document next = states.findOneAndUpdate(session, filter, update, RETURN_AFTER);
            if (next == null) { throw FENCED; }
            return StopReservationDocument.read(proposal.pipelineId(),
                    MongoStateStore.toCheckpoint(next).epoch(),
                    requireMarker(next, proposal.pipelineId()));
        });
    }

    Optional<StopReservation> rebind(StopReservation expected, StopAuthority successor, Instant at) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(successor, "successor");
        Objects.requireNonNull(at, "at");
        if (expected.successor() != null && expected.successor().scope().executionGeneration() != successor.executionGeneration()
                || expected.phase() == StopReservation.Phase.STOPPING && expected.source().scope() != null
                        && expected.source().scope().executionGeneration() != successor.executionGeneration()) {
            return Optional.empty();
        }
        StopReservation rebound = expected.rebind(successor, Math.addExact(expected.reservedEpoch(), 1));
        Document marker = StopReservationDocument.write(rebound);
        return transact(expected.pipelineId(), session -> {
            requireCurrent(session, expected);
            guardDesired(session, expected.pipelineId(), expected.originalDesired());
            guardAuthority(session, expected.pipelineId(), rebound.source(), successor);
            Document next = states.findOneAndUpdate(session, exactMarker(expected),
                    new Document("$set", new Document(StopReservationDocument.FIELD, marker)
                            .append("touchMillis", at.toEpochMilli()))
                            .append("$inc", new Document("epoch", 1L)), RETURN_AFTER);
            if (next == null) { throw FENCED; }
            return StopReservationDocument.read(expected.pipelineId(), MongoStateStore.toCheckpoint(next).epoch(),
                    requireMarker(next, expected.pipelineId()));
        });
    }

    Optional<StopReservation> replace(StopReservation expected, StopReservation successor, Instant at) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(successor, "successor");
        Objects.requireNonNull(at, "at");
        String previousCluster = expected.source().clusterId();
        String successorCluster = successor.source().clusterId();
        if (!expected.pipelineId().equals(successor.pipelineId())
                || expected.token().equals(successor.token())
                || successor.sourceEpoch() != expected.reservedEpoch()
                || successor.reservedEpoch() != Math.addExact(expected.reservedEpoch(), 1)
                || expected.originalDesired().equals(successor.originalDesired())
                || !previousCluster.equals(successorCluster)) {
            throw new IllegalArgumentException("a successor stop must replace the exact epoch with a fresh intent and token");
        }
        Document marker = StopReservationDocument.write(successor);
        return transact(expected.pipelineId(), session -> {
            requireCurrent(session, expected);
            guardDesired(session, expected.pipelineId(), successor.originalDesired());
            if (!successor.legacy() && successor.counterPolicy() != StopReservation.CounterPolicy.freeze(
                    MongoStateStore.toCheckpoint(state(session, expected.pipelineId())), successor.originalDesired())) {
                throw new IllegalArgumentException("replacement policy requires the exact actual state proof");
            }
            guardAuthority(session, expected.pipelineId(), successor.source(), successor.writerAuthority());
            Document next = states.findOneAndUpdate(session, exactMarker(expected),
                    new Document("$set", new Document(StopReservationDocument.FIELD, marker)
                            .append("touchMillis", at.toEpochMilli()))
                            .append("$inc", new Document("epoch", 1L)), RETURN_AFTER);
            if (next == null) { throw FENCED; }
            return StopReservationDocument.read(expected.pipelineId(), MongoStateStore.toCheckpoint(next).epoch(),
                    requireMarker(next, expected.pipelineId()));
        });
    }

    Optional<CheckpointDoc> complete(StopReservation expected, Instant at) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(at, "at");
        if (expected.phase() != StopReservation.Phase.STOPPING || !expected.legacy()
                && expected.originalDesired().targetState() == PipelineState.RUNNING) {
            throw new IllegalArgumentException("ordinary completion cannot discard a replacement handoff");
        }
        return transact(expected.pipelineId(), session -> {
            requireCurrent(session, expected);
            guardDesired(session, expected.pipelineId(), expected.originalDesired());
            guardAuthority(session, expected.pipelineId(), expected.source(), expected.writerAuthority());
            Document next = states.findOneAndUpdate(session, exactMarker(expected),
                    new Document("$set", new Document("stateJson", StateJson.of(PipelineState.STOPPED))
                            .append("touchMillis", at.toEpochMilli()))
                            .append("$unset", new Document(StopReservationDocument.FIELD, true))
                            .append("$inc", new Document("epoch", 1L)), RETURN_AFTER);
            if (next == null) { throw FENCED; }
            return MongoStateStore.toCheckpoint(next);
        });
    }

    Optional<CheckpointDoc> retire(StopReservation expected, DesiredState successor,
            StopAuthority authority, Instant at) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(successor, "successor");
        Objects.requireNonNull(at, "at");
        if (!expected.pipelineId().equals(successor.pipelineId())
                || expected.originalDesired().equals(successor)
                || authority == null && (expected.writerAuthority() != null || expected.successor() != null
                        || expected.source().oldJob() != null)) {
            throw new IllegalArgumentException("retirement requires the superseding intent and current authority");
        }
        String clusterId = expected.source().clusterId();
        if (authority != null && !clusterId.equals(authority.clusterId())) {
            throw new IllegalArgumentException("retirement authority belongs to another cluster");
        }
        return transact(expected.pipelineId(), session -> {
            requireCurrent(session, expected);
            guardDesired(session, expected.pipelineId(), successor);
            guardAuthority(session, expected.pipelineId(), expected.source(), authority);
            Document next = states.findOneAndUpdate(session, exactMarker(expected),
                    new Document("$unset", new Document(StopReservationDocument.FIELD, true))
                            .append("$set", new Document("touchMillis", at.toEpochMilli()))
                            .append("$inc", new Document("epoch", 1L)), RETURN_AFTER);
            if (next == null) { throw FENCED; }
            return MongoStateStore.toCheckpoint(next);
        });
    }

    Optional<StopReservation> promote(StopReservation expected, DesiredState currentIntent,
            StopAuthority writer, Instant at) {
        if (!expected.legacy()) { throw new IllegalArgumentException("only a legacy marker can be promoted"); }
        return transact(expected.pipelineId(), session -> {
            requireCurrent(session, expected);
            guardDesired(session, expected.pipelineId(), currentIntent);
            guardAuthority(session, expected.pipelineId(), expected.source(), writer);
            CheckpointDoc actual = MongoStateStore.toCheckpoint(state(session, expected.pipelineId()));
            StopReservation.Source source = expected.source();
            if (source.scope() == null && expected.writerAuthority() != null && writer != null
                    && expected.writerAuthority().executionGeneration() == writer.executionGeneration()
                    && writer.executionGeneration() > 0 && artifacts != null) {
                String incarnation = artifactIncarnation(session, expected.pipelineId());
                if (incarnation != null) {
                    guardArtifact(session, expected.pipelineId(), incarnation);
                    source = new StopReservation.Source(source.clusterId(),
                            new ObservationStore.Scope(incarnation, writer.executionGeneration()), null);
                }
            }
            if (source.scope() != null && (writer == null
                    || source.scope().executionGeneration() != writer.executionGeneration())) { throw FENCED; }
            StopReservation.CounterPolicy policy;
            try { policy = StopReservation.CounterPolicy.freeze(actual, expected.originalDesired()); }
            catch (RuntimeException corrupt) { throw unreadable(expected.pipelineId(), "stateJson"); }
            StopReservation promoted = moved(expected, StopReservation.Phase.STOPPING, source, policy, writer, null);
            return writeMarker(session, expected, promoted, null, at);
        });
    }

    Optional<StopReservation> replacementPending(StopReservation expected, Instant at) {
        requirePhase(expected, StopReservation.Phase.STOPPING);
        if (expected.originalDesired().targetState() != PipelineState.RUNNING) {
            throw new IllegalArgumentException("replacement requires the original running intent");
        }
        return transact(expected.pipelineId(), session -> {
            guardOriginal(session, expected);
            StopReservation next = moved(expected, StopReservation.Phase.REPLACEMENT_PENDING,
                    expected.source(), expected.counterPolicy(), expected.writerAuthority(), null);
            return writeMarker(session, expected, next, StateJson.of(PipelineState.STOPPED), at);
        });
    }

    Optional<SuccessorAdmission> admit(StopReservation expected, String incarnation, String boot, Instant at) {
        return admit(expected, incarnation, boot, Optional.empty(), at);
    }

    Optional<SuccessorAdmission> admit(StopReservation expected, String incarnation, String boot,
            Set<String> executionMembers, Instant at) {
        Objects.requireNonNull(executionMembers, "executionMembers");
        if (expected.writerAuthority() != null && !expected.writerAuthority().standalone()
                && executionMembers.isEmpty()) {
            throw new IllegalArgumentException("a clustered successor needs its factual planned members");
        }
        return admit(expected, incarnation, boot, Optional.of(Set.copyOf(executionMembers)), at);
    }

    private Optional<SuccessorAdmission> admit(StopReservation expected, String incarnation, String boot,
            Optional<Set<String>> executionMembers, Instant at) {
        requirePhase(expected, StopReservation.Phase.REPLACEMENT_PENDING);
        Objects.requireNonNull(incarnation, "incarnation"); Objects.requireNonNull(boot, "boot");
        if (incarnation.isBlank() || boot.isBlank()) { throw new IllegalArgumentException("admission identities are blank"); }
        return transact(expected.pipelineId(), session -> {
            guardOriginal(session, expected);
            if (!StateJson.of(PipelineState.STOPPED).equals(state(session, expected.pipelineId()).get("stateJson"))) {
                throw FENCED;
            }
            if (expected.source().scope() != null
                    && !incarnation.equals(expected.source().scope().pipelineIncarnationId())) { throw FENCED; }
            guardArtifact(session, expected.pipelineId(), incarnation);
            StopAuthority prior = expected.writerAuthority();
            Document advanced = prior != null && prior.claim() != null
                    ? (executionMembers.isPresent() ? generations.advanceUnderClaim(session, prior.claim(),
                            prior.claim().topologyRevision(), executionMembers.orElseThrow())
                            : generations.advanceUnderClaim(session, prior.claim(), prior.claim().topologyRevision()))
                            .orElseThrow(() -> FENCED)
                    : generations.advanceStandalone(session, expected.source().clusterId(), expected.pipelineId()).orElseThrow(() -> FENCED);
            validateClaim(advanced, expected.pipelineId(), true);
            long generation = ((Number) advanced.get("executionGeneration")).longValue();
            if (generation < 1) { throw unreadable(expected.pipelineId(), "workloadClaims.executionGeneration"); }
            Optional<WorkloadClaim> claim = prior != null && prior.claim() != null
                    ? Optional.of(MongoWorkloadClaimStore.read(advanced)) : Optional.empty();
            StopAuthority current = claim.map(value -> StopAuthority.claimed(WorkloadClaimFence.from(value)))
                    .orElseGet(() -> StopAuthority.standalone(expected.source().clusterId(), generation));
            StopReservation.Successor slot = new StopReservation.Successor(
                    new ObservationStore.Scope(incarnation, generation), boot, null);
            StopReservation next = moved(expected, StopReservation.Phase.SUCCESSOR_ADMITTED,
                    expected.source(), expected.counterPolicy(), current, slot);
            // True BSON encoding is inside the transaction, so an oversized slot aborts its generation too.
            StopReservation accepted = writeMarker(session, expected, next, null, at);
            return new SuccessorAdmission(accepted, claim);
        });
    }

    Optional<StopReservation> bind(StopReservation expected, ObservationStore.Scope scope,
            StopReservation.JobIdentity job, Instant at) {
        requirePhase(expected, StopReservation.Phase.SUCCESSOR_ADMITTED);
        Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(job, "job");
        StopReservation.Successor slot = expected.successor();
        if (!slot.scope().equals(scope) || !slot.submissionBootId().equals(job.bootId())
                || !expected.source().clusterId().equals(job.clusterId())) { return Optional.empty(); }
        return transact(expected.pipelineId(), session -> {
            guardOriginal(session, expected);
            guardArtifact(session, expected.pipelineId(), scope.pipelineIncarnationId());
            StopReservation next = moved(expected, StopReservation.Phase.SUCCESSOR_BOUND,
                    expected.source(), expected.counterPolicy(), expected.writerAuthority(),
                    new StopReservation.Successor(slot.scope(), slot.submissionBootId(), job));
            return writeMarker(session, expected, next, StateJson.of(PipelineState.RUNNING), at);
        });
    }

    Optional<StopReservation> retireSuccessor(StopReservation expected, SuccessorEnd end, Instant at) {
        if (expected.legacy() || expected.successor() == null) {
            throw new IllegalArgumentException("retirement requires an occupied successor slot");
        }
        Objects.requireNonNull(end, "end");
        StopReservation.Successor slot = expected.successor();
        boolean matches = switch (end) {
            case SuccessorEnd.Absent absent -> expected.phase() == StopReservation.Phase.SUCCESSOR_ADMITTED
                    && slot.scope().equals(absent.scope()) && slot.submissionBootId().equals(absent.submissionBootId());
            case SuccessorEnd.Terminal terminal -> expected.phase() == StopReservation.Phase.SUCCESSOR_BOUND
                    && slot.scope().equals(terminal.scope()) && slot.job().equals(terminal.job());
        };
        if (!matches) { return Optional.empty(); }
        return transact(expected.pipelineId(), session -> {
            guardOriginal(session, expected);
            StopReservation next = moved(expected, StopReservation.Phase.REPLACEMENT_PENDING,
                    expected.source(), expected.counterPolicy(), expected.writerAuthority(), null);
            return writeMarker(session, expected, next, StateJson.of(PipelineState.STOPPED), at);
        });
    }

    Optional<StopReservation> recordTerminal(StopReservation expected, SuccessorEnd.Terminal end,
            PipelineState terminal, Instant at) {
        requirePhase(expected, StopReservation.Phase.SUCCESSOR_BOUND);
        Objects.requireNonNull(end, "end"); Objects.requireNonNull(terminal, "terminal");
        if (terminal != PipelineState.FAILED && terminal != PipelineState.COMPLETED) {
            throw new IllegalArgumentException("only factual completed or failed successors are terminal");
        }
        if (!expected.successor().scope().equals(end.scope()) || !expected.successor().job().equals(end.job())) {
            return Optional.empty();
        }
        return transact(expected.pipelineId(), session -> {
            guardOriginal(session, expected);
            String current = state(session, expected.pipelineId()).getString("stateJson");
            String desiredTerminal = StateJson.of(terminal);
            if (desiredTerminal.equals(current)) { return expected; }
            if (!StateJson.of(PipelineState.RUNNING).equals(current)) { throw FENCED; }
            return writeMarker(session, expected, moved(expected, StopReservation.Phase.SUCCESSOR_BOUND,
                    expected.source(), expected.counterPolicy(), expected.writerAuthority(), expected.successor()),
                    desiredTerminal, at);
        });
    }

    Optional<CheckpointDoc> completeHandoff(StopReservation expected, HandoffIdentity ready, Instant at) {
        requirePhase(expected, StopReservation.Phase.SUCCESSOR_BOUND);
        if (expected.counterPolicy() == StopReservation.CounterPolicy.CONTINUE
                && !expected.handoffIdentity().equals(ready)
                || expected.counterPolicy() == StopReservation.CounterPolicy.RESET
                        && ready != null && !expected.handoffIdentity().equals(ready)) { return Optional.empty(); }
        return transact(expected.pipelineId(), session -> {
            guardOriginal(session, expected);
            guardArtifact(session, expected.pipelineId(), expected.successor().scope().pipelineIncarnationId());
            String actual = state(session, expected.pipelineId()).getString("stateJson");
            if (!StateJson.of(PipelineState.RUNNING).equals(actual) && !StateJson.of(PipelineState.FAILED).equals(actual)
                    && !StateJson.of(PipelineState.COMPLETED).equals(actual)) { throw FENCED; }
            Document next = states.findOneAndUpdate(session,
                    exactMarker(expected).append("stateJson", actual),
                    new Document("$unset", new Document(StopReservationDocument.FIELD, true))
                            .append("$set", new Document("touchMillis", at.toEpochMilli()))
                            .append("$inc", new Document("epoch", 1L)), RETURN_AFTER);
            if (next == null) { throw FENCED; }
            return MongoStateStore.toCheckpoint(next);
        });
    }

    Optional<CheckpointDoc> failReplacement(StopReservation expected,
            Optional<StopReservation.JobIdentity> factualJob, Instant at) {
        Objects.requireNonNull(expected, "expected"); Objects.requireNonNull(factualJob, "factualJob");
        Objects.requireNonNull(at, "at");
        if (expected.legacy() || expected.phase() != StopReservation.Phase.REPLACEMENT_PENDING
                && expected.phase() != StopReservation.Phase.SUCCESSOR_ADMITTED) {
            throw new IllegalArgumentException("replacement refusal requires the pending or admitted marker");
        }
        StopReservation.Successor slot = expected.successor();
        if (factualJob.isPresent() && (slot == null
                || !slot.submissionBootId().equals(factualJob.orElseThrow().bootId())
                || !expected.source().clusterId().equals(factualJob.orElseThrow().clusterId())
                || factualJob.orElseThrow().equals(expected.source().oldJob()))) {
            return Optional.empty();
        }
        return transact(expected.pipelineId(), session -> {
            guardOriginal(session, expected);
            if (!StateJson.of(PipelineState.STOPPED).equals(state(session, expected.pipelineId()).get("stateJson"))) {
                throw FENCED;
            }
            String incarnation = slot != null ? slot.scope().pipelineIncarnationId()
                    : expected.source().scope() != null ? expected.source().scope().pipelineIncarnationId()
                            : artifactIncarnation(session, expected.pipelineId());
            if (incarnation == null) { throw FENCED; }
            guardArtifact(session, expected.pipelineId(), incarnation);
            if (factualJob.isPresent()) {
                StopReservation bound = moved(expected, StopReservation.Phase.SUCCESSOR_BOUND,
                        expected.source(), expected.counterPolicy(), expected.writerAuthority(),
                        new StopReservation.Successor(slot.scope(), slot.submissionBootId(), factualJob.orElseThrow()));
                writeMarker(session, expected, bound, StateJson.of(PipelineState.FAILED), at);
                return MongoStateStore.toCheckpoint(state(session, expected.pipelineId()));
            }
            Document failed = states.findOneAndUpdate(session,
                    exactMarker(expected).append("stateJson", StateJson.of(PipelineState.STOPPED)),
                    new Document("$set", new Document("stateJson", StateJson.of(PipelineState.FAILED))
                            .append("touchMillis", at.toEpochMilli()))
                            .append("$unset", new Document(StopReservationDocument.FIELD, true))
                            .append("$inc", new Document("epoch", 1L)), RETURN_AFTER);
            if (failed == null) { throw FENCED; }
            return MongoStateStore.toCheckpoint(failed);
        });
    }

    private static void requirePhase(StopReservation marker, StopReservation.Phase phase) {
        Objects.requireNonNull(marker, "marker");
        if (marker.legacy() || marker.phase() != phase) {
            throw new IllegalArgumentException("handoff phase does not permit this transition");
        }
    }

    private void guardOriginal(ClientSession session, StopReservation expected) {
        requireCurrent(session, expected);
        guardDesired(session, expected.pipelineId(), expected.originalDesired());
        guardAuthority(session, expected.pipelineId(), expected.source(), expected.writerAuthority());
        if (artifacts != null && expected.source().scope() != null) {
            guardArtifact(session, expected.pipelineId(), expected.source().scope().pipelineIncarnationId());
        }
    }

    private static StopReservation moved(StopReservation expected, StopReservation.Phase phase,
            StopReservation.Source source, StopReservation.CounterPolicy policy,
            StopAuthority writer, StopReservation.Successor successor) {
        return new StopReservation(expected.pipelineId(), expected.token(), expected.sourceEpoch(),
                Math.incrementExact(expected.reservedEpoch()), expected.originalDesired(), source, phase, policy,
                writer, successor, StopReservation.CURRENT_FORMAT);
    }

    private StopReservation writeMarker(ClientSession session, StopReservation expected,
            StopReservation next, String stateJson, Instant at) {
        Document fields = new Document(StopReservationDocument.FIELD, StopReservationDocument.write(next))
                .append("touchMillis", at.toEpochMilli());
        if (stateJson != null) { fields.append("stateJson", stateJson); }
        Document applied = states.findOneAndUpdate(session, exactMarker(expected),
                new Document("$set", fields).append("$inc", new Document("epoch", 1L)), RETURN_AFTER);
        if (applied == null) { throw FENCED; }
        return StopReservationDocument.read(expected.pipelineId(), MongoStateStore.toCheckpoint(applied).epoch(),
                requireMarker(applied, expected.pipelineId()));
    }

    private String artifactIncarnation(ClientSession session, String id) {
        if (artifacts == null) { return null; }
        Document artifact = artifacts.find(session, new Document("_id", id).append("kind", "pipeline")).first();
        if (artifact == null || !artifact.containsKey("pipelineIncarnationId")) { return null; }
        Object raw = artifact.get("pipelineIncarnationId");
        if (!(raw instanceof String value) || value.isBlank()) { throw unreadable(id, "pipelineIncarnationId"); }
        return value;
    }

    private void guardArtifact(ClientSession session, String id, String incarnation) {
        if (artifacts == null) { throw new UnsupportedOperationException("handoff admission requires artifact fencing"); }
        Document filter = new Document("_id", id).append("kind", "pipeline").append("pipelineIncarnationId", incarnation);
        if (artifacts.updateOne(session, filter, PROVE_DESIRED).getMatchedCount() != 1) { throw FENCED; }
    }

    /** Runs only cold storage commands after all lifecycle guards have written in the same transaction. */
    <T> Optional<T> withExpectedHandoff(StopReservation expected, Function<ClientSession, T> coldWrites) {
        Objects.requireNonNull(expected, "expected"); Objects.requireNonNull(coldWrites, "coldWrites");
        return transact(expected.pipelineId(), session -> {
            guardOriginal(session, expected);
            return coldWrites.apply(session);
        });
    }

    /** A cold delete cannot race an active continuation or create a phantom checkpoint as its fence. */
    <T> Optional<T> withNoContinuationHandoff(String id, Function<ClientSession, T> coldWrites) {
        Objects.requireNonNull(id, "id"); Objects.requireNonNull(coldWrites, "coldWrites");
        return transact(id, session -> {
            Document observed = state(session, id);
            if (observed == null) { throw FENCED; }
            CheckpointDoc checkpoint = MongoStateStore.toCheckpoint(observed);
            Document marker = requireMarkerOrAbsent(observed, id);
            if (marker != null) {
                StopReservation handoff = StopReservationDocument.read(id, checkpoint.epoch(), marker);
                StopReservation.CounterPolicy policy;
                try {
                    policy = handoff.legacy() ? StopReservation.CounterPolicy.freeze(checkpoint, handoff.originalDesired())
                            : handoff.counterPolicy();
                } catch (RuntimeException corrupt) { throw unreadable(id, "stateJson"); }
                if (policy == StopReservation.CounterPolicy.CONTINUE) { throw FENCED; }
            }
            Object previousGuard = observed.get("stopGuardVersion");
            if (previousGuard != null && !(previousGuard instanceof Long || previousGuard instanceof Integer)
                    || previousGuard instanceof Number number && number.longValue() < 0) {
                throw unreadable(id, "stopGuardVersion");
            }
            Document filter = new Document("_id", id).append("epoch", checkpoint.epoch())
                    .append("stateJson", checkpoint.stateJson()).append(StopReservationDocument.FIELD,
                            marker == null ? new Document("$exists", false) : marker);
            if (states.updateOne(session, filter, new Document("$inc", new Document("stopGuardVersion", 1L)))
                    .getMatchedCount() != 1) { throw FENCED; }
            return coldWrites.apply(session);
        });
    }

    /** A conditional cold write uses this sentinel to abort, rather than commit partial guard writes. */
    static RuntimeException fencedHandoff() { return FENCED; }

    /** A refusal commits only while its original actual, intent, artifacts and writer still qualify. */
    <T> Optional<T> withPreExecutionFailure(io.tapstate.spi.store.PreExecutionFailure.Receipt receipt,
            Function<ClientSession, T> coldWrites) {
        Objects.requireNonNull(receipt, "receipt"); Objects.requireNonNull(coldWrites, "coldWrites");
        return transact(receipt.owner().pipelineId(), session -> {
            var owner = receipt.owner();
            String id = owner.pipelineId();
            Document actual = state(session, id);
            if (actual == null || !MongoStateStore.toCheckpoint(actual).equals(receipt.checkpoint())) { throw FENCED; }
            Document marker = requireMarkerOrAbsent(actual, id);
            guardDesired(session, id, receipt.desired());
            if (states.updateOne(session, new Document("_id", id).append("epoch", receipt.checkpoint().epoch())
                    .append("stateJson", receipt.checkpoint().stateJson())
                    .append(StopReservationDocument.FIELD, marker == null ? new Document("$exists", false) : marker),
                    PROVE_DESIRED).getMatchedCount() != 1) { throw FENCED; }
            if (artifacts == null) { throw new UnsupportedOperationException("refusal publication requires artifact fencing"); }
            for (var artifact : receipt.artifactHashes().entrySet()) {
                Document filter = new Document("_id", artifact.getKey()).append("contentHash", artifact.getValue());
                if (artifact.getKey().equals(id)) {
                    filter.append("kind", "pipeline").append("pipelineIncarnationId", owner.pipelineIncarnationId());
                }
                if (artifacts.updateOne(session, filter, PROVE_DESIRED).getMatchedCount() != 1) { throw FENCED; }
            }
            guardRefusalFrontier(session, owner);
            return coldWrites.apply(session);
        });
    }

    Optional<CheckpointDoc> failPreExecution(io.tapstate.spi.store.PreExecutionFailure.Attempt expected, Instant at) {
        return transact(expected.pipelineId(), session -> {
            String id = expected.pipelineId();
            guardDesired(session, id, expected.desired());
            if (artifacts == null) { throw new UnsupportedOperationException("refusal transitions require artifact fencing"); }
            for (var artifact : expected.artifactHashes().entrySet()) {
                Document filter = new Document("_id", artifact.getKey()).append("contentHash", artifact.getValue());
                if (artifact.getKey().equals(id)) {
                    filter.append("kind", "pipeline").append("pipelineIncarnationId", expected.pipelineIncarnationId());
                }
                if (artifacts.updateOne(session, filter, PROVE_DESIRED).getMatchedCount() != 1) { throw FENCED; }
            }
            guardRefusalFrontier(session, id, expected.clusterId(), expected.generationFrontier(), expected.writer());
            Document failed = states.findOneAndUpdate(session, new Document("_id", id)
                    .append("epoch", expected.originalCheckpoint().epoch()).append("stateJson", expected.originalCheckpoint().stateJson())
                    .append(StopReservationDocument.FIELD, new Document("$exists", false)),
                    new Document("$set", new Document("stateJson", StateJson.of(PipelineState.FAILED)).append("touchMillis", at.toEpochMilli()))
                            .append("$inc", new Document("epoch", 1L)), RETURN_AFTER);
            if (failed == null) { throw FENCED; }
            return MongoStateStore.toCheckpoint(failed);
        });
    }

    /** Payload is already verified; all cold ownership reads use the same Mongo snapshot. */
    boolean currentPreExecutionFailure(io.tapstate.spi.store.PreExecutionFailure.Owner owner,
            java.util.function.Predicate<ClientSession> sameCurrent) {
        return preExecutionFailureReceipt(owner, sameCurrent).isPresent();
    }

    Optional<io.tapstate.spi.store.PreExecutionFailure.Receipt> preExecutionFailureReceipt(
            io.tapstate.spi.store.PreExecutionFailure.Owner owner, java.util.function.Predicate<ClientSession> sameCurrent) {
        Objects.requireNonNull(owner, "owner"); Objects.requireNonNull(sameCurrent, "sameCurrent");
        return transact(owner.pipelineId(), session -> {
            String id = owner.pipelineId();
            Document actual = state(session, id);
            Document wanted = desired.find(session, new Document("_id", id)).first();
            if (actual == null || wanted == null || artifacts == null) { throw FENCED; }
            var checkpoint = MongoStateStore.toCheckpoint(actual);
            if (checkpoint.epoch() != owner.checkpointEpoch()
                    || !owner.checkpointDigest().equals(io.tapstate.spi.store.PreExecutionFailure.checkpointDigest(checkpoint))
                    || !owner.desiredDigest().equals(io.tapstate.spi.store.PreExecutionFailure.desiredDigest(
                            readDesiredExact(wanted, id)))) { throw FENCED; }
            Map<String, String> hashes = new java.util.TreeMap<>();
            java.util.Set<String> visited = new java.util.HashSet<>();
            var pending = new java.util.ArrayDeque<String>(); pending.add(id);
            while (!pending.isEmpty()) {
                String artifactId = pending.removeFirst();
                if (!visited.add(artifactId)) { continue; }
                Document stored = artifacts.find(session, new Document("_id", artifactId)).first();
                if (stored == null) { continue; }
                if (artifactId.equals(id) && (!"pipeline".equals(stored.get("kind"))
                        || !owner.pipelineIncarnationId().equals(stored.get("pipelineIncarnationId")))) { throw FENCED; }
                var resource = MongoArtifactStore.toResource(stored);
                hashes.put(artifactId, io.tapstate.core.model.canonical.CanonicalHash.of(resource));
                io.tapstate.core.dsl.ReferenceGraph.declaredReferences(resource).forEach(pending::addLast);
            }
            if (!hashes.containsKey(id) || !owner.artifactLineageDigest().equals(
                    io.tapstate.spi.store.PreExecutionFailure.lineageDigest(hashes))
                    || !refusalFrontierMatches(session, owner) || !sameCurrent.test(session)) { throw FENCED; }
            return new io.tapstate.spi.store.PreExecutionFailure.Receipt(owner, checkpoint,
                    readDesiredExact(wanted, id), hashes);
        });
    }

    private boolean refusalFrontierMatches(ClientSession session, io.tapstate.spi.store.PreExecutionFailure.Owner owner) {
        return refusalFrontierMatches(session, owner.pipelineId(), owner.clusterId(), owner.generationFrontier(), owner.writer());
    }

    private boolean refusalFrontierMatches(ClientSession session, String id, String clusterId,
            java.util.OptionalLong frontier, WorkloadClaimFence writer) {
        Document stored = claims.find(session, new Document("_id", claimId(clusterId, id))).first();
        if (stored == null) { return writer == null && frontier.isEmpty(); }
        validateClaim(stored, id, false);
        long actual = stored.get("executionGeneration") instanceof Number number ? number.longValue() : 0L;
        if (actual != frontier.orElse(0L)) { return false; }
        if (writer == null) {
            return claims.find(session, new Document("_id", claimId(clusterId, id))
                    .append("$or", List.of(new Document("ownerNodeId", new Document("$exists", false)),
                            new Document("$expr", new Document("$lte", List.of("$leaseUntil", "$$NOW")))))).first() != null;
        }
        var fence = writer;
        Document filter = new Document("_id", claimId(clusterId, id))
                .append("ownerNodeId", fence.owner().nodeId()).append("ownerBootId", fence.owner().bootId())
                .append("claimGeneration", fence.claimGeneration()).append("executionGeneration", fence.executionGeneration())
                .append("topologyRevision", fence.topologyRevision())
                .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")));
        return claims.find(session, filter).first() != null;
    }

    private void guardRefusalFrontier(ClientSession session, io.tapstate.spi.store.PreExecutionFailure.Owner owner) {
        guardRefusalFrontier(session, owner.pipelineId(), owner.clusterId(), owner.generationFrontier(), owner.writer());
    }

    private void guardRefusalFrontier(ClientSession session, String id, String clusterId,
            java.util.OptionalLong frontier, WorkloadClaimFence writer) {
        if (!refusalFrontierMatches(session, id, clusterId, frontier, writer)) { throw FENCED; }
        if (writer != null) {
            guardKnownAuthority(session, id, StopAuthority.claimed(writer));
        } else if (frontier.isPresent()) {
            guardKnownAuthority(session, id, StopAuthority.standalone(clusterId, frontier.getAsLong()));
        } else {
            Document key = claimId(clusterId, id);
            Document stored = claims.find(session, new Document("_id", key)).first();
            if (stored != null) {
                Document filter = new Document("_id", key).append("executionGeneration", stored.get("executionGeneration"))
                        .append("$or", List.of(new Document("ownerNodeId", new Document("$exists", false)),
                                new Document("$expr", new Document("$lte", List.of("$leaseUntil", "$$NOW")))));
                if (claims.updateOne(session, filter, PROVE_CLAIM).getMatchedCount() != 1) { throw FENCED; }
            }
            // There is no write or upsert for a missing generation document. Manifest role fencing and
            // the post-payload authority snapshot make a concurrent first real admission invalidate it.
        }
    }

    void requirePreExecutionContinuation(ClientSession session, io.tapstate.spi.store.PreExecutionFailure.Owner owner,
            io.tapstate.spi.store.ObservationStore.ContinuationReceipt carried, Document header,
            Document originalCurrent, boolean covered) {
        Document actual = state(session, owner.pipelineId());
        if (actual == null) { throw FENCED; }
        Document raw = requireMarkerOrAbsent(actual, owner.pipelineId());
        if (raw == null) { return; }
        var marker = StopReservationDocument.read(owner.pipelineId(), MongoStateStore.toCheckpoint(actual).epoch(), raw);
        if (marker.legacy()) { throw FENCED; }
        if (marker.counterPolicy() != StopReservation.CounterPolicy.CONTINUE) { return; }
        if (!covered || carried == null || header == null || !Objects.equals(originalCurrent, header.get("current"))
                || !marker.token().equals(carried.token())
                || !Objects.equals(marker.source().scope(), carried.sourceScope())
                || !MongoObservationContinuation.expectedMatches(header, Optional.of(carried))) { throw FENCED; }
        if (marker.successor() != null && carried.target().filter(target ->
                target.scope().equals(marker.successor().scope())
                        && target.realJob().equals(Optional.ofNullable(marker.successor().job()))).isEmpty()) { throw FENCED; }
    }

    private <T> Optional<T> transact(String id, Function<ClientSession, T> action) {
        for (int attempt = 0; attempt < 8; attempt++) {
            try {
                T result;
                try (ClientSession session = client.startSession()) {
                    result = session.withTransaction(() -> action.apply(session), TRANSACTION);
                }
                return Optional.of(result);
            } catch (Fenced lost) {
                return Optional.empty();
            } catch (MongoException failure) {
                if (failure.getCode() == 11000 && attempt < 7) { continue; }
                throw StoreIo.coded(failure);
            }
        }
        throw new IllegalStateException("bounded transaction retry fell through");
    }

    private Document state(ClientSession session, String id) {
        return states.find(session, new Document("_id", id)).first();
    }

    private void requireCurrent(ClientSession session, StopReservation expected) {
        Document current = state(session, expected.pipelineId());
        if (current == null || MongoStateStore.toCheckpoint(current).epoch() != expected.reservedEpoch()) {
            throw FENCED;
        }
        Document marker = requireMarkerOrAbsent(current, expected.pipelineId());
        if (marker == null || !expected.equals(StopReservationDocument.read(expected.pipelineId(),
                expected.reservedEpoch(), marker))) {
            throw FENCED;
        }
    }

    private static Document exactMarker(StopReservation expected) {
        Document filter = new Document("_id", expected.pipelineId()).append("epoch", expected.reservedEpoch())
                .append(StopReservationDocument.FIELD + (expected.legacy() ? ".token" : ".t"), expected.token())
                .append(StopReservationDocument.FIELD + (expected.legacy() ? ".reservedEpoch" : ".e"), expected.reservedEpoch());
        if (!expected.legacy()) { filter.append(StopReservationDocument.FIELD + ".p", expected.phase().name()); }
        return filter;
    }

    private static Document requireMarker(Document state, String id) {
        Document marker = requireMarkerOrAbsent(state, id);
        if (marker == null) { throw FENCED; }
        return marker;
    }

    private static Document requireMarkerOrAbsent(Document state, String id) {
        if (!state.containsKey(StopReservationDocument.FIELD)) { return null; }
        if (!(state.get(StopReservationDocument.FIELD) instanceof Document marker)) {
            throw unreadable(id, StopReservationDocument.FIELD);
        }
        return marker;
    }

    /** The guard update is a real write so a concurrent desired replacement conflicts with this txn. */
    private void guardDesired(ClientSession session, String id, DesiredState expected) {
        Document stored = desired.find(session, new Document("_id", id)).first();
        if (stored == null) { throw FENCED; }
        if (!readDesiredExact(stored, id).equals(expected)) { throw FENCED; }
        Object guard = stored.get("stopGuardVersion");
        if (guard != null && !(guard instanceof Long || guard instanceof Integer)) {
            throw unreadable(id, "stopGuardVersion");
        }
        Document filter = new Document("_id", id)
                .append("targetState", stored.get("targetState"))
                .append("revision", stored.get("revision"))
                .append("purgeState", stored.get("purgeState"))
                .append("assemblyRevision", stored.get("assemblyRevision"))
                .append("reassemble", stored.get("reassemble"))
                .append("rebuiltAtStateEpoch", stored.get("rebuiltAtStateEpoch"));
        if (desired.updateOne(session, filter, PROVE_DESIRED).getMatchedCount() != 1) { throw FENCED; }
    }

    private static DesiredState readDesiredExact(Document stored, String id) {
        if (!(stored.get("_id") instanceof String)
                || !(stored.get("targetState") instanceof String)
                || !(stored.get("revision") instanceof String)
                || stored.containsKey("purgeState") && !(stored.get("purgeState") instanceof Boolean)
                || stored.containsKey("reassemble") && !(stored.get("reassemble") instanceof Boolean)
                || stored.get("assemblyRevision") != null && !(stored.get("assemblyRevision") instanceof String)
                || stored.get("rebuiltAtStateEpoch") != null
                        && !(stored.get("rebuiltAtStateEpoch") instanceof Long
                                || stored.get("rebuiltAtStateEpoch") instanceof Integer)) {
            throw unreadable(id, "pipelineDesired");
        }
        try { return MongoDesiredStore.toDesired(stored); }
        catch (IllegalArgumentException | ClassCastException malformed) { throw unreadable(id, "pipelineDesired"); }
    }

    /** Current authority and this checkpoint transition write in one short Mongo transaction. */
    private void guardAuthority(ClientSession session, String id,
            StopReservation.Source source, StopAuthority chosen) {
        if (chosen != null) {
            guardKnownAuthority(session, id, chosen);
            return;
        }
        if (source.oldJob() != null) {
            throw new IllegalArgumentException("a real old job requires current execution authority");
        }
        guardColdNoJob(session, id, source.clusterId());
    }

    private void guardKnownAuthority(ClientSession session, String id, StopAuthority authority) {
        Document key = claimId(authority.clusterId(), id);
        Document stored = claims.find(session, new Document("_id", key)).first();
        if (stored == null) { throw FENCED; }
        validateClaim(stored, id, true);
        Document filter;
        WorkloadClaimFence fence = authority.claim();
        if (fence != null) {
            if (!id.equals(fence.key().resourceId())) { throw new IllegalArgumentException("claim names another pipeline"); }
            filter = new Document("_id", key)
                    .append("ownerNodeId", fence.owner().nodeId())
                    .append("ownerBootId", fence.owner().bootId())
                    .append("claimGeneration", fence.claimGeneration())
                    .append("executionGeneration", fence.executionGeneration())
                    .append("topologyRevision", fence.topologyRevision())
                    .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")));
        } else {
            filter = new Document("_id", key).append("executionGeneration", authority.executionGeneration())
                    .append("$or", List.of(
                            new Document("ownerNodeId", new Document("$exists", false)),
                            new Document("$and", List.of(
                                    new Document("leaseUntil", new Document("$type", "date")),
                                    new Document("$expr", new Document("$lte", List.of("$leaseUntil", "$$NOW")))))));
        }
        if (claims.updateOne(session, filter, PROVE_CLAIM).getMatchedCount() != 1) { throw FENCED; }
    }

    /** Creates no generation or lease: only the already used claim-write guard on the canonical id. */
    private void guardColdNoJob(ClientSession session, String id, String clusterId) {
        Document key = claimId(clusterId, id);
        Document stored = claims.find(session, new Document("_id", key)).first();
        if (stored != null) { validateClaim(stored, id, false); }
        Document filter = new Document("_id", key)
                .append("ownerNodeId", new Document("$exists", false))
                .append("$or", List.of(
                        new Document("executionGeneration", new Document("$exists", false)),
                        new Document("executionGeneration", 0L)));
        Document identity = new Document("clusterId", clusterId)
                .append("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                .append("resourceId", id);
        Document update = new Document("$setOnInsert", identity)
                .append("$inc", new Document("fencedAppends", 1L));
        try {
            var applied = claims.updateOne(session, filter, update, new UpdateOptions().upsert(true));
            if (applied.getMatchedCount() != 1 && applied.getUpsertedId() == null) { throw FENCED; }
        } catch (MongoException race) {
            if (ErrorCategory.fromErrorCode(race.getCode()) == ErrorCategory.DUPLICATE_KEY) { throw FENCED; }
            throw race;
        }
    }

    private static Document claimId(String clusterId, String id) {
        return MongoExecutionGenerationWrites.id(new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, id));
    }

    private static void validateClaim(Document stored, String id, boolean requireGeneration) {
        if (!(stored.get("clusterId") instanceof String clusterId) || clusterId.isBlank()
                || !WorkloadClaimType.PIPELINE_ACTUATION.name().equals(stored.get("resourceType"))
                || !id.equals(stored.get("resourceId"))) {
            throw unreadable(id, "workloadClaims.identity");
        }
        boolean hasOwner = stored.containsKey("ownerNodeId");
        Object generation = stored.get("executionGeneration");
        if ((requireGeneration || hasOwner) && !(generation instanceof Long || generation instanceof Integer)
                || generation != null && !(generation instanceof Long || generation instanceof Integer)
                || generation instanceof Number number && number.longValue() < 0) {
            throw unreadable(id, "workloadClaims.executionGeneration");
        }
        if (hasOwner != stored.containsKey("ownerBootId")) { throw unreadable(id, "workloadClaims.owner"); }
        if (hasOwner && (!(stored.get("ownerNodeId") instanceof String ownerNode) || ownerNode.isBlank()
                || !(stored.get("ownerBootId") instanceof String ownerBoot) || ownerBoot.isBlank()
                || !(stored.get("claimGeneration") instanceof Long
                        || stored.get("claimGeneration") instanceof Integer)
                || !(stored.get("topologyRevision") instanceof Long
                        || stored.get("topologyRevision") instanceof Integer)
                || !(stored.get("leaseUntil") instanceof java.util.Date)
                || ((Number) stored.get("claimGeneration")).longValue() < 1
                || ((Number) stored.get("topologyRevision")).longValue() < 0)) {
            throw unreadable(id, "workloadClaims.owner");
        }
        if (!hasOwner && (stored.containsKey("claimGeneration")
                || stored.containsKey("topologyRevision") || stored.containsKey("leaseUntil"))) {
            throw unreadable(id, "workloadClaims.owner");
        }
        Object guard = stored.get("fencedAppends");
        if (guard != null && !(guard instanceof Long || guard instanceof Integer)
                || guard instanceof Number number && number.longValue() < 0) {
            throw unreadable(id, "workloadClaims.fencedAppends");
        }
    }

    private static TapstateException unreadable(String id, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", id, "field", field), null);
    }

    private static final class Fenced extends RuntimeException {
        private Fenced() { super(null, null, false, false); }
    }
}
