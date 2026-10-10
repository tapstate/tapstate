package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.ConsumerProgressKind;
import io.tapstate.spi.store.DurableSourceRead;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WriterProgress;
import io.tapstate.spi.store.WriterRun;
import io.tapstate.spi.store.CaptureResumeWitness;
import io.tapstate.spi.store.CaptureResumePreparation;
import io.tapstate.spi.store.CaptureReadAttempt;
import io.tapstate.spi.store.CaptureReadState;
import io.tapstate.spi.store.CaptureStartupProof;
import io.tapstate.spi.store.CaptureStartupFailure;
import io.tapstate.spi.store.ClusterRecoveryPosition;

import java.time.Clock;
import java.time.Instant;
import java.util.Date;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.bson.Document;

/**
 * The MongoDB SRS meta store: one durable coordination document per mining chain — the offset and schema
 * truth that outlive the in-memory change ring — plus one cursor document per consumer pipeline. The chain
 * document is keyed by the mining chain id (as {@code _id}); a consumer document has a compound id naming
 * its chain and pipeline, so each facet advances independently and no consumer cursor can fill the chain
 * document until its own source position can no longer move.
 *
 * <p>A consumer document holds everything belonging to one pipeline rather than to the chain: its read
 * cursor, its acked position, the tables whose initial load its sink has confirmed, and the seam and
 * generation at which its load began. Records written before that split carried those documents under the
 * chain's {@code consumerOffsets} field. The first consumer write migrates them; a chain write that finds
 * an old record already at the endpoint ceiling does the same and retries. The copy is insert-only and the
 * embedded map is cleared only after every cursor has landed, so an interrupted migration loses nothing.
 * That read/copy/clear is one transaction: concurrent migrations serialize before a detach deletes its
 * cursor, so a copier holding an older snapshot cannot restore the departed consumer. The schema history
 * remains an append-only array on the chain document, advanced by an update that keeps the newest entries
 * inside a fixed byte budget. Nullable positions are stored only when present, never as explicit nulls.
 *
 * <p>A write to a consumer document that is already there is one write of its own, and holds nothing once
 * it has returned. Only a write that finds no document takes a transaction, which writes the chain document
 * too, because creating a cursor has to be set against a drop of the chain it would belong to. What a
 * transaction has written stays held until it ends, and one whose writer stopped halfway through it - a
 * member killed mid-write - ends only when the endpoint gives up on it, a minute later by default. Taken on
 * every consumer write, that put every writer of a chain behind any member that died while it was writing.
 *
 * <p>Driver IO failures are translated into coded io diagnostics, so no driver type escapes the module
 * (rule R3). A re-seed of an existing chain (which would discard its accumulated truth) and a mutate of
 * an unseeded chain are caller ordering errors — surfaced bare (an {@code IllegalStateException}), not
 * laundered into an io code that would hide the defect. A stored document that cannot be read back into
 * its model is coded {@code io.document-unreadable}.
 */
public final class MongoSrsMetaStore implements SrsMetaStore {

    private static final TransactionOptions SOURCE_PROOF = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final String PREPARED_SOURCE = "preparedSourceStart";
    private static final String READ_ATTEMPT = "captureReadAttempt";

    private <T> T sourceProof(java.util.function.Function<ClientSession, T> action) {
        return StoreIo.call(() -> {
            try (ClientSession session = client.startSession()) {
                return session.withTransaction(() -> action.apply(session), SOURCE_PROOF);
            }
        });
    }

    @Override
    public CaptureResumeWitness resumeWitness(String sourceId, String connectorId, String chain,
            String consumerId, io.tapstate.core.model.ReadMode mode, boolean shared, List<String> tables) {
        return sourceProof(session -> {
            Document root = collection.find(session, new Document("_id", chain)).projection(new Document("epoch", 1)
                    .append("sourceReadOffset", 1).append("sourceReadEpoch", 1).append("sourceReadSeq", 1)
                    .append("sourceReadDurable", 1).append("consumerOffsets", 1).append(READ_ATTEMPT, 1)).first();
            Document consumer = consumers.find(session, consumerKey(chain, consumerId)).first();
            if (consumer == null && root != null && root.get("consumerOffsets") instanceof Document legacy) {
                Object found = legacy.get(consumerId);
                consumer = found instanceof Document value ? value : null;
            }
            ConsumerOffset offset = consumer == null ? null : consumerFromDocument(consumerId, consumer);
            return new CaptureResumeWitness(sourceId, connectorId, chain, consumerId, mode, shared, tables,
                    root != null, root == null ? 0 : readEpoch(root, "epoch"), root == null ? null : sourceReadFrom(root),
                    root != null && sourceReadDurableFrom(root, chain), offset != null,
                    offset == null ? List.of() : offset.snapshotCompletedTables(), offset == null ? null : offset.cdcStartPosition(),
                    offset == null ? 0 : offset.snapshotEpoch(), offset == null ? null : offset.progressKind(),
                    offset == null ? null : offset.sinkAcked(), offset == null ? Map.of() : offset.sinkAckedByTable(),
                    root == null ? null : CaptureStartupDocuments.reader(root.get(READ_ATTEMPT, Document.class)));
        });
    }

    @Override
    public boolean prepareCaptureResume(WorkloadClaimFence pipeline, CaptureResumeWitness witness,
            ClusterRecoveryPosition requested) {
        return prepareCaptureResume(pipeline, witness, requested, Set.of(witness.sourceId()));
    }

    @Override
    public boolean prepareCaptureResume(WorkloadClaimFence pipeline, CaptureResumeWitness witness,
            ClusterRecoveryPosition requested, Set<String> requiredSources) {
        return prepareCaptureResume(pipeline, witness, requested, requiredSources, null);
    }

    @Override
    public boolean prepareCaptureResume(WorkloadClaimFence pipeline, CaptureResumeWitness witness,
            ClusterRecoveryPosition requested, Set<String> requiredSources, String retention) {
        requirePipelineFence(witness.consumerId(), pipeline);
        if (!requiredSources.contains(witness.sourceId())) {
            throw new IllegalArgumentException("the actual source must belong to the required startup selection");
        }
        List<String> required = requiredSources.stream().sorted().toList();
        return sourceProof(session -> {
            if (!provesClaim(session, pipeline)) { return false; }
            Document key = consumerKey(witness.miningChainId(), witness.consumerId());
            Document existing = consumers.find(session, key).first();
            Document prior = CaptureStartupDocuments.prepared(existing);
            if (prior != null && sameProofRun(prior.get("pipelineClaim", Document.class), pipeline)) {
                return witness.equals(CaptureStartupDocuments.witness(prior.get("witness", Document.class)))
                        && Objects.equals(CaptureStartupDocuments.requested(requested), prior.get("requestedPosition"))
                        && required.equals(prior.get("requiredSourceIds"));
            }
            if (!guardResumeWitness(session, witness, retention)) { return false; }
            Document prepared = new Document("schemaVersion", 1)
                    .append("pipelineClaim", new Document("$literal", WorkloadClaimDocuments.stored(pipeline)))
                    .append("witness", new Document("$literal", CaptureStartupDocuments.witness(witness)))
                    .append("requestedPosition", new Document("$literal", CaptureStartupDocuments.requested(requested)))
                    .append("requiredSourceIds", new Document("$literal", required))
                    .append("preparedAt", "$$NOW").append("snapshotAcceptedAt", null);
            Document fields = consumerIdentity(witness.miningChainId(), witness.consumerId());
            fields.replaceAll((name, value) -> new Document("$literal", value));
            fields.append(PREPARED_SOURCE, prepared).append("perTableSeq", new Document("$ifNull", List.of("$perTableSeq", new Document())));
            consumers.updateOne(session, key, List.of(new Document("$set", fields)), new UpdateOptions().upsert(true));
            return true;
        });
    }

    /** True root and exact-consumer writes serialize a preparation with every subsequent truth change. */
    boolean guardResumeWitness(ClientSession session, CaptureResumeWitness witness) {
        return guardResumeWitness(session, witness, null);
    }

    private boolean guardResumeWitness(ClientSession session, CaptureResumeWitness witness, String retention) {
        String chain = witness.miningChainId();
        Document root = collection.find(session, new Document("_id", chain)).first();
        if ((root != null) != witness.chainPresent()) { return false; }
        if (root == null) {
            collection.insertOne(session, toDocument(new SrsMeta(chain, null, List.of(), List.of(), retention)));
        } else {
            List<Document> conditions = new ArrayList<>();
            conditions.add(eqDefault("epoch", witness.chainEpoch(), 0L));
            conditions.add(eqDefault("sourceReadDurable", witness.sourceReadDurable(), false));
            if (!Objects.equals(CaptureStartupDocuments.reader(root.get(READ_ATTEMPT, Document.class)), witness.priorReader())) {
                return false;
            }
            ChainPosition read = witness.sourceRead();
            conditions.add(eqDefault("sourceReadOffset", read == null ? null : read.token(), null));
            conditions.add(eqDefault("sourceReadEpoch", read == null || read.order() == null ? null : read.order().epoch(), null));
            conditions.add(eqDefault("sourceReadSeq", read == null || read.order() == null ? null : read.order().seq(), null));
            Document filter = new Document("_id", chain).append("$expr", new Document("$and", conditions));
            if (collection.updateOne(session, filter, new Document("$inc", new Document("sourceResumeProofRevision", 1L)))
                    .getMatchedCount() != 1) { return false; }
        }
        Document key = consumerKey(chain, witness.consumerId());
        Document consumer = consumers.find(session, key).first();
        if (consumer == null && root != null && root.get("consumerOffsets") instanceof Document legacy) {
            Object prior = legacy.get(witness.consumerId());
            if (prior instanceof Document original) {
                consumer = new Document(original);
                consumer.putAll(key);
                consumer.putAll(consumerIdentity(chain, witness.consumerId()));
                consumers.insertOne(session, consumer);
                collection.updateOne(session, new Document("_id", chain),
                        new Document("$unset", new Document("consumerOffsets." + witness.consumerId(), "")));
            }
        }
        if ((consumer != null) != witness.consumerPresent()) { return false; }
        if (consumer != null) {
            ConsumerOffset actual = consumerFromDocument(witness.consumerId(), consumer);
            if (!actual.snapshotCompletedTables().equals(witness.snapshotCompletedTables())
                    || !Objects.equals(actual.cdcStartPosition(), witness.cdcStartPosition())
                    || actual.snapshotEpoch() != witness.snapshotEpoch() || actual.progressKind() != witness.progressKind()
                    || !Objects.equals(actual.sinkAcked(), witness.sinkAcked())
                    || !actual.sinkAckedByTable().equals(witness.sinkAckedByTable())) { return false; }
            return consumers.updateOne(session, key, new Document("$inc", new Document("sourceResumeProofRevision", 1L)))
                    .getMatchedCount() == 1;
        }
        return true;
    }

    @Override
    public Optional<CaptureResumePreparation> captureResumePreparation(WorkloadClaimFence pipeline, String chain, String consumerId) {
        return StoreIo.call(() -> {
            Document row = consumers.withReadConcern(ReadConcern.MAJORITY).find(consumerKey(chain, consumerId))
                    .projection(new Document(PREPARED_SOURCE, 1)).first();
            Document prepared = CaptureStartupDocuments.prepared(row);
            return prepared == null || !sameProofRun(prepared.get("pipelineClaim", Document.class), pipeline) ? Optional.empty()
                    : Optional.of(new CaptureResumePreparation(pipeline, CaptureStartupDocuments.witness(prepared.get("witness", Document.class)),
                            CaptureStartupDocuments.requested(prepared.get("requestedPosition", Document.class)),
                            CaptureStartupDocuments.instant(prepared, "preparedAt"), CaptureStartupDocuments.requiredSources(prepared)));
        });
    }

    @Override
    public List<CaptureResumePreparation> captureResumePreparations(WorkloadClaimFence pipeline) {
        return StoreIo.call(() -> {
            Document filter = new Document();
            WorkloadClaimDocuments.stored(pipeline).forEach((field, value) -> {
                if (!field.equals("topologyRevision")) { filter.put(PREPARED_SOURCE + ".pipelineClaim." + field, value); }
            });
            List<CaptureResumePreparation> found = new ArrayList<>();
            for (Document row : consumers.withReadConcern(ReadConcern.MAJORITY).find(filter)
                    .projection(new Document(PREPARED_SOURCE, 1))) {
                Document prepared = CaptureStartupDocuments.prepared(row);
                found.add(new CaptureResumePreparation(pipeline, CaptureStartupDocuments.witness(prepared.get("witness", Document.class)),
                        CaptureStartupDocuments.requested(prepared.get("requestedPosition", Document.class)),
                        CaptureStartupDocuments.instant(prepared, "preparedAt"), CaptureStartupDocuments.requiredSources(prepared)));
            }
            return List.copyOf(found);
        });
    }

    @Override
    public Optional<CaptureReadAttempt> beginCaptureReadAttempt(String chain, long epoch, List<String> tables,
            CaptureReadAttempt.Kind kind, String token, Instant instant, WorkloadClaimFence capture) {
        if (capture.key().type() != WorkloadClaimType.CAPTURE || capture.profileGeneration() < 1) {
            throw new IllegalArgumentException("a durable reader attempt requires a profile-aware capture claim");
        }
        return sourceProof(session -> {
            if (!provesClaim(session, capture)) { return Optional.empty(); }
            Document version = new Document("$add", List.of(new Document("$ifNull", List.of("$captureReadVersion", 0L)), 1L));
            Document attempt = new Document("schemaVersion", 1).append("miningChainId", new Document("$literal", chain))
                    .append("chainEpoch", epoch).append("version", version)
                    .append("captureClaim", new Document("$literal", WorkloadClaimDocuments.stored(capture)))
                    .append("tables", new Document("$literal", List.copyOf(tables)))
                    .append("requestedKind", kind.name()).append("requestedToken", new Document("$literal", token))
                    .append("requestedInstant", instant == null ? null : Date.from(instant)).append("allocatedAt", "$$NOW")
                    .append("resolvedAnchor", null).append("anchorResolvedAt", null).append("firstDeliveredAt", null)
                    .append("failed", false).append("failureCode", null)
                    .append("failureParams", new Document("$literal", new Document())).append("disposition", null)
                    .append("failedAt", null).append("eventRevision", 0L).append("failureRevision", 0L)
                    .append("failureClaimProven", false);
            Document updated = collection.findOneAndUpdate(session, new Document("_id", chain).append("epoch", epoch),
                    List.of(new Document("$set", new Document("captureReadVersion", version).append(READ_ATTEMPT, attempt))),
                    new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
            return Optional.ofNullable(updated).map(row -> CaptureStartupDocuments.reader(row.get(READ_ATTEMPT, Document.class)).attempt());
        });
    }

    @Override
    public boolean bindCaptureReadAttempt(WorkloadClaimFence pipeline, CaptureResumeWitness witness,
            CaptureReadAttempt attempt, boolean attachment) {
        return sourceProof(session -> {
            if (!provesClaim(session, pipeline) || !provesClaim(session, attempt.captureClaim())
                    || pipeline.profileGeneration() != attempt.captureClaim().profileGeneration()
                    || !pipeline.key().clusterId().equals(attempt.captureClaim().key().clusterId())
                    || !attempt.miningChainId().equals(witness.miningChainId())
                    || !attempt.tables().containsAll(witness.tables()) || (attachment && !witness.srsEnabled())) { return false; }
            Document root = collection.find(session, readerFilter(attempt)).first();
            if (root == null || (witness.srsEnabled() && (!attempt.tables().equals(root.get("captureServingTables"))
                    || readEpoch(root, "captureServingEpoch") != attempt.chainEpoch()))) { return false; }
            Document key = consumerKey(witness.miningChainId(), witness.consumerId());
            Document row = consumers.find(session, key).first();
            Document prepared = CaptureStartupDocuments.prepared(row);
            if (prepared == null || !sameProofRun(prepared.get("pipelineClaim", Document.class), pipeline)
                    || !witness.equals(CaptureStartupDocuments.witness(prepared.get("witness", Document.class)))) { return false; }
            ClusterRecoveryPosition requested = CaptureStartupDocuments.requested(prepared.get("requestedPosition", Document.class));
            if (!attachment) {
                if (requested != null && requested.position() != null && requested.position().token() != null
                        && (attempt.requestedKind() != CaptureReadAttempt.Kind.RESUME
                            || !requested.position().token().equals(attempt.requestedToken()))) { return false; }
                if (requested != null && requested.kind() == ClusterRecoveryPosition.Kind.SNAPSHOT_REQUIRED
                        && witness.readMode() == io.tapstate.core.model.ReadMode.SNAPSHOT_AND_CDC) {
                    ConsumerOffset actual = consumerFromDocument(witness.consumerId(), row);
                    if (attempt.requestedKind() != CaptureReadAttempt.Kind.RESUME || actual.snapshotEpoch() < 1
                            || !Objects.equals(actual.cdcStartPosition(), attempt.requestedToken())) { return false; }
                    requested = new ClusterRecoveryPosition(witness.sourceId(), witness.connectorId(), requested.captureId(),
                            ClusterRecoveryPosition.Kind.DURABLE_POSITION,
                            new ChainPosition(SourceOrder.snapshotRow(actual.snapshotEpoch()), attempt.requestedToken()),
                            "mongo-snapshot-seam", witness.miningChainId() + ":" + witness.consumerId());
                }
            }
            if (collection.updateOne(session, readerFilter(attempt), new Document("$inc",
                    new Document("sourceStartupProofRevision", 1L))).getMatchedCount() != 1) { return false; }
            boolean sameReader = readEpoch(prepared, "readerVersion") == attempt.version()
                    && Objects.equals(prepared.get("readerClaim"), WorkloadClaimDocuments.stored(attempt.captureClaim()));
            long observedRevision = sameReader ? readEpoch(prepared, "readerObservedRevision")
                    : readEpoch(root.get(READ_ATTEMPT, Document.class), "eventRevision");
            Document fields = new Document(PREPARED_SOURCE + ".readerObservedRevision", observedRevision)
                    .append(PREPARED_SOURCE + ".readerVersion", attempt.version())
                    .append(PREPARED_SOURCE + ".readerEpoch", attempt.chainEpoch())
                    .append(PREPARED_SOURCE + ".readerClaim", new Document("$literal", WorkloadClaimDocuments.stored(attempt.captureClaim())))
                    .append(PREPARED_SOURCE + ".readerTables", new Document("$literal", attempt.tables()))
                    .append(PREPARED_SOURCE + ".sharedAttachment", attachment)
                    .append(PREPARED_SOURCE + ".requestedPosition", new Document("$literal", CaptureStartupDocuments.requested(requested)));
            return consumers.updateOne(session, key, List.of(new Document("$set", fields))).getMatchedCount() == 1;
        });
    }

    @Override
    public boolean recordCaptureAnchor(CaptureReadAttempt attempt, String anchor) {
        Objects.requireNonNull(anchor, "anchor");
        return updateReader(attempt, new Document("$set", new Document(READ_ATTEMPT + ".resolvedAnchor", new Document("$literal", anchor))
                .append(READ_ATTEMPT + ".anchorResolvedAt", "$$NOW")), true);
    }

    @Override
    public boolean recordCaptureFirstDelivery(CaptureReadAttempt attempt) {
        return updateReader(attempt, new Document("$set", new Document(READ_ATTEMPT + ".firstDeliveredAt",
                new Document("$ifNull", List.of("$" + READ_ATTEMPT + ".firstDeliveredAt", "$$NOW")))), false);
    }

    @Override
    public boolean recordCaptureReadFailure(CaptureReadAttempt attempt, String code) {
        return recordCaptureReadFailure(attempt, code, Map.of(), "retry-source-start");
    }

    @Override
    public boolean recordCaptureReadFailure(CaptureReadAttempt attempt, String code,
            Map<String, Object> params, String disposition) {
        return sourceProof(session -> {
            if (!provesClaim(session, attempt.captureClaim())) { return false; }
            Document next = new Document("$add", List.of(new Document("$ifNull",
                    List.of("$" + READ_ATTEMPT + ".eventRevision", 0L)), 1L));
            Document fields = new Document(READ_ATTEMPT + ".failed", true)
                    .append(READ_ATTEMPT + ".failureCode", new Document("$literal", code))
                    .append(READ_ATTEMPT + ".failureParams", new Document("$literal", CaptureStartupDocuments.namedParams(params)))
                    .append(READ_ATTEMPT + ".disposition", new Document("$literal", disposition))
                    .append(READ_ATTEMPT + ".failedAt", "$$NOW")
                    .append(READ_ATTEMPT + ".failureClaimProven", true)
                    .append(READ_ATTEMPT + ".failureRevision", next).append(READ_ATTEMPT + ".eventRevision", next);
            return collection.updateOne(session, readerFilter(attempt).append(READ_ATTEMPT + ".failed", false),
                    List.of(new Document("$set", fields))).getMatchedCount() == 1;
        });
    }

    private boolean updateReader(CaptureReadAttempt attempt, Document update, boolean withoutAnchor) {
        return sourceProof(session -> {
            if (!provesClaim(session, attempt.captureClaim())) { return false; }
            Document filter = readerFilter(attempt);
            if (!withoutAnchor) { filter.append(READ_ATTEMPT + ".resolvedAnchor", new Document("$type", "string")); }
            if (!update.get("$set", Document.class).containsKey(READ_ATTEMPT + ".failed")) {
                filter.append(READ_ATTEMPT + ".failed", false);
            }
            if (update.get("$set", Document.class).containsKey(READ_ATTEMPT + ".resolvedAnchor")) {
                Object literal = update.get("$set", Document.class).get(READ_ATTEMPT + ".resolvedAnchor", Document.class).get("$literal");
                filter.append("$or", List.of(new Document(READ_ATTEMPT + ".resolvedAnchor", null),
                        new Document(READ_ATTEMPT + ".resolvedAnchor", literal)));
            }
            return collection.updateOne(session, filter, List.of(update)).getMatchedCount() == 1;
        });
    }

    @Override
    public Optional<CaptureReadState> captureReadState(String chain) {
        return StoreIo.call(() -> {
            Document root = collection.withReadConcern(ReadConcern.MAJORITY).find(new Document("_id", chain))
                    .projection(new Document(READ_ATTEMPT, 1)).first();
            return root == null ? Optional.empty() : Optional.ofNullable(CaptureStartupDocuments.reader(root.get(READ_ATTEMPT, Document.class)));
        });
    }

    @Override
    public boolean recordCaptureAttachment(WorkloadClaimFence pipeline, CaptureResumeWitness witness, long epoch) {
        return sourceProof(session -> {
            if (!provesClaim(session, pipeline)) { return false; }
            Document root = collection.find(session, new Document("_id", witness.miningChainId()).append("epoch", epoch)).first();
            Document row = consumers.find(session, consumerKey(witness.miningChainId(), witness.consumerId())).first();
            Document prepared = CaptureStartupDocuments.prepared(row);
            if (root == null || prepared == null || !sameProofRun(prepared.get("pipelineClaim", Document.class), pipeline)
                    || !witness.equals(CaptureStartupDocuments.witness(prepared.get("witness", Document.class)))) { return false; }
            if (witness.srsEnabled() && (!row.containsKey("perTableSeq")
                    || !row.get("perTableSeq", Document.class).keySet().containsAll(witness.tables()))) { return false; }
            if (collection.updateOne(session, new Document("_id", witness.miningChainId()).append("epoch", epoch),
                    new Document("$inc", new Document("sourceStartupProofRevision", 1L))).getMatchedCount() != 1) { return false; }
            Document fields = new Document(PREPARED_SOURCE + ".attachedEpoch", epoch)
                    .append(PREPARED_SOURCE + ".attachedAt", new Document("$ifNull",
                            List.of("$" + PREPARED_SOURCE + ".attachedAt", "$$NOW")));
            CaptureReadState reader = CaptureStartupDocuments.reader(root.get(READ_ATTEMPT, Document.class));
            if (reader != null && reader.attempt().chainEpoch() == epoch
                    && reader.attempt().tables().containsAll(witness.tables())
                    && reader.attempt().captureClaim().profileGeneration() == pipeline.profileGeneration()
                    && reader.attempt().captureClaim().key().clusterId().equals(pipeline.key().clusterId())
                    && provesClaim(session, reader.attempt().captureClaim())) {
                boolean sameReader = readEpoch(prepared, "readerVersion") == reader.attempt().version()
                        && Objects.equals(prepared.get("readerClaim"), WorkloadClaimDocuments.stored(reader.attempt().captureClaim()))
                        && CaptureStartupDocuments.instant(prepared, "attachedAt") != null;
                fields.append(PREPARED_SOURCE + ".readerVersion", reader.attempt().version())
                        .append(PREPARED_SOURCE + ".readerEpoch", epoch)
                        .append(PREPARED_SOURCE + ".readerClaim", new Document("$literal", WorkloadClaimDocuments.stored(reader.attempt().captureClaim())))
                        .append(PREPARED_SOURCE + ".readerTables", new Document("$literal", reader.attempt().tables()))
                        .append(PREPARED_SOURCE + ".readerObservedRevision", sameReader
                                ? readEpoch(prepared, "readerObservedRevision")
                                : readEpoch(root.get(READ_ATTEMPT, Document.class), "eventRevision"));
                if (witness.srsEnabled()) { fields.append(PREPARED_SOURCE + ".sharedAttachment", true); }
            }
            return consumers.updateOne(session, consumerKey(witness.miningChainId(), witness.consumerId()),
                    List.of(new Document("$set", fields))).getMatchedCount() == 1;
        });
    }

    @Override
    public boolean recordSnapshotStartup(WorkloadClaimFence pipeline, CaptureResumeWitness witness) {
        if (!witness.snapshotOwed()) { return false; }
        return sourceProof(session -> {
            if (!provesClaim(session, pipeline)) { return false; }
            Document key = consumerKey(witness.miningChainId(), witness.consumerId());
            Document row = consumers.find(session, key).first();
            Document prepared = CaptureStartupDocuments.prepared(row);
            if (prepared == null || !sameProofRun(prepared.get("pipelineClaim", Document.class), pipeline)
                    || !witness.equals(CaptureStartupDocuments.witness(prepared.get("witness", Document.class)))) { return false; }
            Document fields = new Document(PREPARED_SOURCE + ".snapshotAcceptedAt", new Document("$ifNull",
                    List.of("$" + PREPARED_SOURCE + ".snapshotAcceptedAt", "$$NOW")));
            ClusterRecoveryPosition requested = CaptureStartupDocuments.requested(prepared.get("requestedPosition", Document.class));
            if (witness.readMode() != io.tapstate.core.model.ReadMode.SNAPSHOT_ONLY && requested != null
                    && requested.kind() == ClusterRecoveryPosition.Kind.SNAPSHOT_REQUIRED) {
                ConsumerOffset actual = consumerFromDocument(witness.consumerId(), row);
                if (actual.cdcStartPosition() == null || actual.snapshotEpoch() < 1) { return false; }
                ClusterRecoveryPosition actualSeam = new ClusterRecoveryPosition(witness.sourceId(), witness.connectorId(),
                        requested.captureId(), ClusterRecoveryPosition.Kind.DURABLE_POSITION,
                        new ChainPosition(SourceOrder.snapshotRow(actual.snapshotEpoch()), actual.cdcStartPosition()),
                        "mongo-snapshot-seam", witness.miningChainId() + "/" + witness.consumerId());
                fields.append(PREPARED_SOURCE + ".requestedPosition", new Document("$literal", CaptureStartupDocuments.requested(actualSeam)));
            }
            return consumers.updateOne(session, key, List.of(new Document("$set", fields))).getMatchedCount() == 1;
        });
    }

    @Override
    public Optional<CaptureStartupProof> captureStartupProof(WorkloadClaimFence pipeline, CaptureResumeWitness witness) {
        return sourceProof(session -> startupProof(session, pipeline, witness, false));
    }

    @Override
    public Optional<CaptureStartupFailure> captureStartupFailure(WorkloadClaimFence pipeline, CaptureResumeWitness witness) {
        return sourceProof(session -> startupFailure(session, pipeline, witness));
    }

    boolean guardStartupFailure(ClientSession session, CaptureStartupFailure fact, WorkloadClaimFence pipeline) {
        return fact.pipelineClaim().equals(pipeline)
                && startupFailure(session, pipeline, fact.witness()).filter(fact::equals).isPresent();
    }

    boolean guardStartupFailure(ClientSession session, CaptureStartupFailure fact, WorkloadClaimFence pipeline,
            Set<String> requiredSources) {
        Document row = consumers.find(session, consumerKey(fact.witness().miningChainId(), fact.witness().consumerId())).first();
        Document prepared = CaptureStartupDocuments.prepared(row);
        return prepared != null && CaptureStartupDocuments.requiredSources(prepared).equals(requiredSources)
                && guardStartupFailure(session, fact, pipeline);
    }

    private Optional<CaptureStartupFailure> startupFailure(ClientSession session,
            WorkloadClaimFence pipeline, CaptureResumeWitness witness) {
        if (!provesClaim(session, pipeline)) { return Optional.empty(); }
        Document row = consumers.find(session, consumerKey(witness.miningChainId(), witness.consumerId())).first();
        Document prepared = CaptureStartupDocuments.prepared(row);
        Document root = collection.find(session, new Document("_id", witness.miningChainId())).first();
        CaptureReadState reader = root == null ? null : CaptureStartupDocuments.reader(root.get(READ_ATTEMPT, Document.class));
        if (prepared == null || !sameProofRun(prepared.get("pipelineClaim", Document.class), pipeline)
                || !witness.equals(CaptureStartupDocuments.witness(prepared.get("witness", Document.class)))
                || reader == null || !reader.failed() || reader.failureCode() == null || reader.failedAt() == null
                || !pipeline.key().clusterId().equals(reader.attempt().captureClaim().key().clusterId())
                || pipeline.profileGeneration() != reader.attempt().captureClaim().profileGeneration()
                || reader.disposition() == null || CaptureStartupDocuments.instant(prepared, "attachedAt") == null
                || readEpoch(prepared, "attachedEpoch") != reader.attempt().chainEpoch()
                || readEpoch(root, "epoch") != reader.attempt().chainEpoch()
                || readEpoch(prepared, "readerVersion") != reader.attempt().version()
                || !Objects.equals(prepared.get("readerClaim"), WorkloadClaimDocuments.stored(reader.attempt().captureClaim()))
                || !Objects.equals(prepared.get("readerTables"), reader.attempt().tables())
                || !reader.attempt().tables().containsAll(witness.tables())
                || readEpoch(root.get(READ_ATTEMPT, Document.class), "failureRevision")
                    <= readEpoch(prepared, "readerObservedRevision")
                || (witness.srsEnabled() && (!reader.attempt().tables().equals(root.get("captureServingTables"))
                    || readEpoch(root, "captureServingEpoch") != reader.attempt().chainEpoch()))
                || !provesRecordedFailure(session, reader, root.get(READ_ATTEMPT, Document.class))) { return Optional.empty(); }
        if (collection.updateOne(session, readerFilter(reader.attempt()).append(READ_ATTEMPT + ".failed", true),
                new Document("$inc", new Document("sourceStartupProofRevision", 1L))).getMatchedCount() != 1
                || consumers.updateOne(session, new Document("_id", row.get("_id")).append(PREPARED_SOURCE, prepared),
                new Document("$inc", new Document("sourceStartupProofRevision", 1L))).getMatchedCount() != 1) {
            return Optional.empty();
        }
        return Optional.of(new CaptureStartupFailure(pipeline, witness,
                CaptureStartupDocuments.requested(prepared.get("requestedPosition", Document.class)),
                CaptureStartupDocuments.instant(prepared, "preparedAt"), reader));
    }

    private boolean provesRecordedFailure(ClientSession session, CaptureReadState reader, Document marker) {
        Object proven = marker.get("failureClaimProven");
        if (proven != null && !(proven instanceof Boolean)) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", reader.attempt().miningChainId(), "field", "captureReadAttempt.failureClaimProven"), null);
        }
        // A committed first failure was guarded while its exact capture claim was live. Cleanup may
        // retire that claim; the immutable cause remains qualified by the current pipeline and reader.
        // Older records without this proof still require a fresh live capture-claim condition-write.
        return Boolean.TRUE.equals(proven) || provesClaim(session, reader.attempt().captureClaim());
    }

    /** Consumes the stored pre-open and accepted facts, never a later mutable checkpoint observation. */
    boolean guardPreparedStartup(ClientSession session, WorkloadClaimFence pipeline, CaptureResumeWitness witness,
            ClusterRecoveryPosition requested) {
        Optional<CaptureStartupProof> proof = startupProof(session, pipeline, witness, true);
        return proof.isPresent() && Objects.equals(proof.get().requestedPosition(), requested);
    }

    boolean guardPreparedStartup(ClientSession session, WorkloadClaimFence pipeline, CaptureResumeWitness witness,
            ClusterRecoveryPosition requested, Set<String> requiredSources) {
        Document row = consumers.find(session, consumerKey(witness.miningChainId(), witness.consumerId())).first();
        Document prepared = CaptureStartupDocuments.prepared(row);
        return prepared != null && CaptureStartupDocuments.requiredSources(prepared).equals(requiredSources)
                && guardPreparedStartup(session, pipeline, witness, requested);
    }

    private Optional<CaptureStartupProof> startupProof(ClientSession session, WorkloadClaimFence pipeline,
            CaptureResumeWitness witness, boolean touch) {
        if (!provesClaim(session, pipeline)) { return Optional.empty(); }
        Document row = consumers.find(session, consumerKey(witness.miningChainId(), witness.consumerId())).first();
        Document prepared = CaptureStartupDocuments.prepared(row);
        if (prepared == null || !sameProofRun(prepared.get("pipelineClaim", Document.class), pipeline)
                || !witness.equals(CaptureStartupDocuments.witness(prepared.get("witness", Document.class)))) { return Optional.empty(); }
        CaptureReadState reader = null;
        Instant accepted;
        if (witness.readMode() == io.tapstate.core.model.ReadMode.SNAPSHOT_ONLY) {
            accepted = CaptureStartupDocuments.instant(prepared, "snapshotAcceptedAt");
            if (accepted == null) { return Optional.empty(); }
        } else {
            Document root = collection.find(session, new Document("_id", witness.miningChainId())).first();
            reader = root == null ? null : CaptureStartupDocuments.reader(root.get(READ_ATTEMPT, Document.class));
            if (reader == null || !reader.accepted() || !reader.attempt().tables().containsAll(witness.tables())
                    || !pipeline.key().clusterId().equals(reader.attempt().captureClaim().key().clusterId())
                    || pipeline.profileGeneration() != reader.attempt().captureClaim().profileGeneration()
                    || CaptureStartupDocuments.instant(prepared, "attachedAt") == null
                    || readEpoch(prepared, "attachedEpoch") != reader.attempt().chainEpoch()
                    || (witness.snapshotOwed() && CaptureStartupDocuments.instant(prepared, "snapshotAcceptedAt") == null)
                    || readEpoch(prepared, "readerVersion") != reader.attempt().version()
                    || readEpoch(prepared, "readerEpoch") != reader.attempt().chainEpoch()
                    || !Objects.equals(prepared.get("readerClaim"), WorkloadClaimDocuments.stored(reader.attempt().captureClaim()))
                    || !Objects.equals(prepared.get("readerTables"), reader.attempt().tables())
                    || (witness.srsEnabled() && (!reader.attempt().tables().equals(root.get("captureServingTables"))
                        || readEpoch(root, "captureServingEpoch") != reader.attempt().chainEpoch()))
                    || readEpoch(root, "epoch") != reader.attempt().chainEpoch()
                    || !provesClaim(session, reader.attempt().captureClaim())) { return Optional.empty(); }
            if (touch && collection.updateOne(session, readerFilter(reader.attempt())
                            .append(READ_ATTEMPT + ".failed", false)
                            .append(READ_ATTEMPT + ".firstDeliveredAt", Date.from(reader.firstDeliveredAt())),
                    new Document("$inc", new Document("sourceStartupProofRevision", 1L))).getMatchedCount() != 1) {
                return Optional.empty();
            }
            accepted = reader.firstDeliveredAt();
            Instant attached = CaptureStartupDocuments.instant(prepared, "attachedAt");
            if (attached.isAfter(accepted)) { accepted = attached; }
            Instant snapshot = CaptureStartupDocuments.instant(prepared, "snapshotAcceptedAt");
            if (snapshot != null && snapshot.isAfter(accepted)) { accepted = snapshot; }
        }
        ClusterRecoveryPosition requested = CaptureStartupDocuments.requested(prepared.get("requestedPosition", Document.class));
        if (reader != null && !Boolean.TRUE.equals(prepared.getBoolean("sharedAttachment"))
                && reader.attempt().requestedKind() == CaptureReadAttempt.Kind.RESUME
                && !Objects.equals(reader.attempt().requestedToken(), reader.resolvedAnchor())) { return Optional.empty(); }
        if (requested == null && reader != null) {
            requested = new ClusterRecoveryPosition(witness.sourceId(), witness.connectorId(), reader.attempt().captureClaim().key().resourceId(),
                    ClusterRecoveryPosition.Kind.DURABLE_POSITION,
                    new ChainPosition(SourceOrder.snapshotRow(reader.attempt().chainEpoch()), reader.resolvedAnchor()),
                    "source-resolved-" + reader.attempt().requestedKind().name().toLowerCase(java.util.Locale.ROOT),
                    witness.miningChainId() + "/" + witness.consumerId());
            Document boundary = CaptureStartupDocuments.requested(requested);
            if (consumers.updateOne(session, new Document("_id", row.get("_id")).append(PREPARED_SOURCE, prepared),
                    new Document("$set", new Document(PREPARED_SOURCE + ".requestedPosition", boundary))).getMatchedCount() != 1) {
                return Optional.empty();
            }
            prepared.put("requestedPosition", boundary);
        }
        if (touch && consumers.updateOne(session, new Document("_id", row.get("_id"))
                        .append(PREPARED_SOURCE, prepared), new Document("$inc", new Document("sourceStartupProofRevision", 1L)))
                .getMatchedCount() != 1) { return Optional.empty(); }
        return Optional.of(new CaptureStartupProof(pipeline, witness,
                requested, CaptureStartupDocuments.instant(prepared, "preparedAt"), accepted, reader));
    }

    private static Document readerFilter(CaptureReadAttempt attempt) {
        return new Document("_id", attempt.miningChainId()).append("epoch", attempt.chainEpoch())
                .append(READ_ATTEMPT + ".version", attempt.version()).append(READ_ATTEMPT + ".tables", attempt.tables())
                .append(READ_ATTEMPT + ".captureClaim", WorkloadClaimDocuments.stored(attempt.captureClaim()));
    }

    private static boolean sameProofRun(Document stored, WorkloadClaimFence current) {
        if (stored == null) { return false; }
        Document prior = new Document(stored); prior.remove("topologyRevision");
        Document actual = WorkloadClaimDocuments.stored(current); actual.remove("topologyRevision");
        return prior.equals(actual);
    }

    private static Document eqDefault(String field, Object expected, Object fallback) {
        return new Document("$eq", java.util.Arrays.asList(new Document("$ifNull", java.util.Arrays.asList("$" + field, fallback)),
                new Document("$literal", expected)));
    }

    /**
     * A root-local fence advanced by every split-cursor write that may create the cursor. It has no model
     * meaning: its write is what makes the root-existence check conflict with a concurrent lifecycle delete.
     */
    private static final String CONSUMER_WRITE_REVISION = "consumerWriteRevision";

    /**
     * Per table, the ring sequence up to which this consumer has nothing left to receive -- the last change
     * its sink confirmed there, or where the ring stood when it arrived. What a run of it carries on from.
     * Raised with the chain's acked position and dropped with the rest of the consumer when its record is
     * rewritten, as a write-back that lets the acks go does.
     */
    static final String PER_TABLE_RING_DONE = "perTableRingDone";

    /**
     * Per table, how far the consumer has durably landed it - the order, and the token of the change there where
     * it carried one: what a run replacing this one resumes the table from, and what the next confirmation of
     * the table is compared with. Kept in the shape of the acked position itself.
     */
    static final String SINK_ACKED_BY_TABLE = "sinkAckedByTable";

    /** What the consumer's progress is measured against, which decides the positions a resume may rely on. */
    private static final String PROGRESS_KIND = "progressKind";

    private static final String POSITION = "position";
    private static final String POSITION_ORDER = "position order";
    private static final String TABLE = "table";
    private static final String PIPELINE_ID = "pipelineId";

    /**
     * How much of a chain's schema history the record retains, in bytes of stored entries.
     *
     * <p>The record is one document, and the endpoint refuses a write whose result passes its 16 MiB
     * ceiling — while the history is the one facet that grows for the life of a chain, by an entry per
     * DDL it has ever seen. Left unbounded it arrives at a state where the only write that can record a
     * schema change is a write that cannot land, and the chain then cannot say that its source's schema
     * moved: not a slow read, a chain stuck.
     *
     * <p>Bytes rather than a count of entries, because bytes are what the ceiling counts. An entry is a
     * table's field schema, and a wide table's is a hundred times a narrow one's, so a count that holds
     * the record under the ceiling at one entry size does not at another. A sixteenth of the ceiling
     * leaves the rest of the chain record — its positions and structural fields — ample room, and is a
     * window hundreds of versions long at the entry size this product's records carry.
     */
    private static final long SCHEMA_HISTORY_BUDGET_BYTES = 1024L * 1024L;

    /**
     * What one history entry costs the array beyond its own bytes: the key, which is the element's index,
     * and the type byte. A margin rather than an accounting — the key widens by a digit every tenfold —
     * and stated as a bound, so an entry is never credited with fewer bytes than it occupies and what is
     * retained stays inside the budget rather than touching it.
     */
    private static final int HISTORY_ENTRY_OVERHEAD_BYTES = 12;

    /** The exact pipeline run a consumer's durable sink effects are bound to, beside the progress they make. */
    static final String SINK_ACK_FENCE = "sinkAckFence";

    /** A real write, so a takeover and a fenced sink effect conflict on the same claim document. */
    private static final Document PROVE_SINK_CLAIM =
            new Document("$inc", new Document("fencedSinkEffects", 1L));

    private final MongoCollection<Document> collection;
    private final MongoCollection<Document> consumers;
    private final MongoCollection<Document> workloadClaims;
    private final MongoClient client;
    private final Clock clock;

    public MongoSrsMetaStore(MongoClient client, MongoCollection<Document> collection) {
        this(client, collection, collection, workloadClaims(client, collection), Clock.systemUTC());
    }

    /** The same store reading a given clock, for a caller that needs the recorded time to be decidable. */
    public MongoSrsMetaStore(MongoClient client, MongoCollection<Document> collection, Clock clock) {
        this(client, collection, collection, workloadClaims(client, collection), clock);
    }

    /** A store whose chain roots and per-consumer cursors live in their declared collections. */
    public MongoSrsMetaStore(
            MongoClient client, MongoCollection<Document> collection, MongoCollection<Document> consumers) {
        this(client, collection, consumers, workloadClaims(client, collection), Clock.systemUTC());
    }

    /** A store using the supplied coordination collection for atomic sink-claim proofs. */
    public MongoSrsMetaStore(
            MongoClient client,
            MongoCollection<Document> collection,
            MongoCollection<Document> consumers,
            MongoCollection<Document> workloadClaims) {
        this(client, collection, consumers, workloadClaims, Clock.systemUTC());
    }

    /** The same two-collection store reading a given clock. */
    MongoSrsMetaStore(MongoClient client, MongoCollection<Document> collection,
            MongoCollection<Document> consumers, Clock clock) {
        this(client, collection, consumers, workloadClaims(client, collection), clock);
    }

    private MongoSrsMetaStore(
            MongoClient client,
            MongoCollection<Document> collection,
            MongoCollection<Document> consumers,
            MongoCollection<Document> workloadClaims,
            Clock clock) {
        this.client = Objects.requireNonNull(client, "client");
        this.collection = Objects.requireNonNull(collection, "collection");
        this.consumers = Objects.requireNonNull(consumers, "consumers");
        this.workloadClaims = Objects.requireNonNull(workloadClaims, "workloadClaims");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** The declared claim collection beside {@code collection}, used by convenience constructors. */
    private static MongoCollection<Document> workloadClaims(
            MongoClient client, MongoCollection<Document> collection) {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(collection, "collection");
        return SystemCollections.WORKLOAD_CLAIMS.on(
                client.getDatabase(collection.getNamespace().getDatabaseName()));
    }

    @Override
    public Optional<DurableSourceRead> durableSourceRead(String miningChainId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Document root = StoreIo.call(() -> collection.withReadConcern(ReadConcern.MAJORITY)
                .find(new Document("_id", miningChainId))
                .projection(Projections.include(
                        "sourceReadOffset", "sourceReadEpoch", "sourceReadSeq", "sourceReadDurable"))
                .first());
        if (root == null) {
            return Optional.empty();
        }
        ChainPosition position = sourceReadFrom(root);
        return position == null ? Optional.empty()
                : Optional.of(new DurableSourceRead(position, sourceReadDurableFrom(root, miningChainId)));
    }

    @Override
    public Optional<SrsMeta> read(String miningChainId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", miningChainId)).first());
        if (document == null) {
            return Optional.empty();
        }
        SrsMeta chain = toMeta(document);
        return Optional.of(new SrsMeta(
                chain.miningChainId(),
                chain.sourceRead(),
                mergedConsumers(document),
                chain.schemaHistory(),
                chain.retention(),
                chain.epoch(),
                chain.sourceReadAt(), chain.sourceReadDurable()));
    }

    /**
     * Fetches the consumer cursors alone, asking the chain document only for its legacy field and reading
     * the split cursor documents without carrying the schema history over the wire.
     *
     * <p>This exists because of what it does not carry back. The record's schema history grows by one
     * entry per DDL, up to the bound the record keeps it under, and the cdc write path -- which reads
     * this on every run of changes -- never looks at it. Measured against a real endpoint on a chain with
     * 500 DDLs behind it, the whole record is 671 KB and reads at 6.4 ms, while this projection reads at
     * 0.5 ms and does not move as the history grows.
     *
     * <p>It cannot go through the shared reconstruction: that one requires the schema history to be
     * present and reports a document without it as corruption, which is the right reading there and the
     * wrong one here, where its absence was asked for.
     */
    @Override
    public List<ConsumerOffset> consumerOffsets(String miningChainId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", miningChainId))
                .projection(Projections.include("consumerOffsets"))
                .first());
        if (document == null) {
            return List.of();
        }
        return mergedConsumers(document);
    }

    @Override
    public void create(String miningChainId, String retention) {
        // Insert-only: insertOne fails on a duplicate _id, so an existing chain's accumulated offset /
        // cursor / schema truth is never discarded by a re-seed.
        Document document = toDocument(new SrsMeta(miningChainId, null, List.of(), List.of(), retention));
        try {
            collection.insertOne(document);
        } catch (MongoException e) {
            throw classifyInsertFailure(e, miningChainId);
        }
    }

    @Override
    public void advanceSourceReadOffset(String miningChainId, ChainPosition position) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(position.order(), "position order");
        // Two updates, and the split is the guard. The first carries the ordering condition in its own
        // filter, so the comparison and the write are one atomic act: a read-then-write would let a second
        // member land its advance in between and be overwritten by this one, which is the rewind this
        // exists to stop. It matches nothing when the recorded position already ranks at or after this one.
        long matched = writeChainWithConsumerMigration(miningChainId, () -> collection.updateOne(
                sourceReadAdvanceFilter(miningChainId, position.order()),
                new Document("$set", sourceReadFields(position, Instant.now(clock))
                        .append("sourceReadDurable", false))).getMatchedCount());
        if (matched > 0) {
            return;
        }
        // Nothing matched, which is either "the chain is not seeded" -- a caller ordering error the other
        // mutators raise too -- or "this position does not move the chain forward", which is ordinary and
        // silent. Only a second look tells them apart, and it runs on the path that changed nothing.
        requireSeeded(miningChainId);
    }

    @Override
    public void advanceCaptureCheckpoint(String miningChainId, ChainPosition position) {
        throw new UnsupportedOperationException("a durable capture checkpoint requires its served table selection");
    }

    @Override
    public void advanceCaptureCheckpoint(
            String miningChainId, ChainPosition position, List<String> servedTables) {
        Objects.requireNonNull(position, POSITION);
        Objects.requireNonNull(position.order(), POSITION_ORDER);
        List<String> selection = List.copyOf(Objects.requireNonNull(servedTables, "servedTables"));
        Document fields = sourceReadFields(position, Instant.now(clock));
        fields.append("sourceReadDurable", true);
        Document filter = sourceReadAdvanceFilter(miningChainId, position.order());
        filter.append("epoch", position.order().epoch())
                .append("captureServingEpoch", position.order().epoch()).append("$expr",
                new Document("$and", List.of(new Document("$setIsSubset", List.of(
                        new Document("$ifNull", List.of("$captureTables", List.of())),
                        new Document("$ifNull", List.of("$captureServingTables", List.of())))),
                        new Document("$setIsSubset", List.of(
                                new Document("$ifNull", List.of("$captureTables", List.of())),
                                new Document("$literal", selection))))));
        long matched = writeChainWithConsumerMigration(miningChainId, () -> collection.updateOne(
                filter,
                new Document("$set", fields)).getMatchedCount());
        if (matched == 0) {
            requireSeeded(miningChainId);
        }
    }

    @Override
    public void requestCaptureTables(String miningChainId, List<String> tables) {
        applyToSeeded(miningChainId, () -> collection.updateOne(new Document("_id", miningChainId),
                new Document("$addToSet", new Document("captureTables",
                        new Document("$each", List.copyOf(tables))))));
    }

    @Override
    public List<String> captureTables(String miningChainId) {
        Document root = StoreIo.call(() -> collection.find(new Document("_id", miningChainId))
                .projection(Projections.include("captureTables")).first());
        return root == null || root.get("captureTables") == null ? List.of()
                : List.copyOf(root.getList("captureTables", String.class));
    }

    @Override
    public List<String> captureServingTables(String miningChainId) {
        Document root = StoreIo.call(() -> collection.find(new Document("_id", miningChainId))
                .projection(Projections.include("epoch", "captureServingEpoch", "captureServingTables")).first());
        if (root == null || readEpoch(root, "epoch") != readEpoch(root, "captureServingEpoch")
                || root.get("captureServingTables") == null) {
            return List.of();
        }
        return List.copyOf(root.getList("captureServingTables", String.class));
    }

    @Override
    public boolean publishCaptureTables(String miningChainId, long epoch, List<String> tables) {
        Document filter = new Document("_id", miningChainId).append("epoch", epoch).append("$expr",
                new Document("$setIsSubset", List.of(
                        new Document("$ifNull", List.of("$captureTables", List.of())), List.copyOf(tables))));
        return writeChainWithConsumerMigration(miningChainId, () -> collection.updateOne(filter,
                new Document("$set", new Document("captureServingEpoch", epoch)
                        .append("captureServingTables", List.copyOf(tables)))).getMatchedCount()) > 0;
    }

    @Override
    public void rewindSourceReadOffset(String miningChainId, String token) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(token, "token");
        // One unconditional update, and the unset is half of what it does. The order recorded beside the
        // token says where the engine observed that token in the ring, and this token was not observed
        // here at all -- leaving the old one in place would have the record claim the new position sits
        // exactly where the old one did, in the comparison that decides what is safe to forget.
        long matched = writeChainWithConsumerMigration(miningChainId, () -> collection.updateOne(
                        new Document("_id", miningChainId),
                        new Document("$set", new Document("sourceReadOffset", token)
                                .append("sourceReadAt", Instant.now(clock).toEpochMilli()))
                                .append("$unset", new Document("sourceReadEpoch", "")
                                        .append("sourceReadSeq", "").append("sourceReadDurable", "")))
                .getMatchedCount());
        if (matched == 0) {
            // The filter names the chain and nothing else, so nothing matching can only mean no record.
            requireSeeded(miningChainId);
        }
    }

    /**
     * The filter that admits an advance: this chain, and a recorded position strictly before {@code order}
     * — no record yet, or a lower generation, or the same generation and a lower sequence. Positions are
     * ranked by generation first because a rebuilt ring numbers its sequences from zero again, so a
     * sequence alone is only meaningful within the ring that assigned it.
     */
    static Document sourceReadAdvanceFilter(String miningChainId, SourceOrder order) {
        return new Document("_id", miningChainId).append("$or", List.of(
                new Document("sourceReadEpoch", new Document("$exists", false)),
                new Document("sourceReadEpoch", new Document("$lt", order.epoch())),
                new Document("sourceReadEpoch", order.epoch())
                        .append("sourceReadSeq", new Document("$lt", order.seq()))));
    }

    /**
     * The fields a write to the read offset lays down: the order it reached, the token, and when it was
     * written. An advance carries both halves of the position — a token stored without its order can no
     * longer be ranked, and an order without its token is nothing a read can resume from — so each part
     * is written only when it is there, and after a rewind the order is the part that is not.
     */
    private static Document sourceReadFields(ChainPosition position, Instant at) {
        Document fields = new Document();
        if (position.order() != null) {
            fields.append("sourceReadEpoch", position.order().epoch())
                    .append("sourceReadSeq", position.order().seq());
        }
        if (position.token() != null) {
            fields.append("sourceReadOffset", position.token());
        }
        if (at != null) {
            fields.append("sourceReadAt", at.toEpochMilli());
        }
        return fields;
    }

    /** Raises the caller ordering error the advancing mutators share when a chain has no record. */
    private void requireSeeded(String miningChainId) {
        Document existing = StoreIo.call(() -> collection.find(new Document("_id", miningChainId)).first());
        if (existing == null) {
            throw unseededChain(miningChainId);
        }
    }

    @Override
    public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
        Objects.requireNonNull(offset, "offset");
        migrateLegacyConsumers(miningChainId, true);
        writeConsumer(miningChainId, session -> consumers.replaceOne(session,
                consumerKey(miningChainId, offset.pipelineId()),
                consumerDocument(miningChainId, offset),
                new ReplaceOptions().upsert(true)));
    }

    @Override
    public void advanceConsumerReadSeq(String miningChainId, String pipelineId, String table, long lastReadSeq) {
        updateConsumer(miningChainId, pipelineId, consumerReadSeqUpdate(pipelineId, table, lastReadSeq));
    }

    @Override
    public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
        updateConsumer(miningChainId, pipelineId, sinkAckedUpdate(pipelineId, position));
    }

    /**
     * Writes the whole update while {@code position} is later than the last one written for {@code table}, and
     * otherwise only the part of it that only ever rises: the table's place in its ring.
     *
     * <p>Every writer of a sink reports on its own, working the table's position out from what it read back, so
     * a report worked out before a later one can land after it, carrying the older answer; written, it would
     * move the position back, and a run replacing this one would start from there. So the write carries the
     * comparison in its own filter, and lands only where the table's last position is earlier than this one or
     * there is none: the comparison and the write are one act, which no report landing in between can split.
     * Where that finds nothing, the ring place alone is raised, where the last position is this one or later.
     * Only the table's own: each table's ring numbers its changes on its own, so one table's position says
     * nothing about how far another's has got.
     *
     * <p>Where neither finds anything there is no document to compare with - or its table's position went with
     * a rewrite in between - and the fenced path decides again, in a transaction that may create the document.
     */
    @Override
    public void advanceSinkAcked(
            String miningChainId, String pipelineId, String table, ChainPosition position) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(position.order(), "position order");
        migrateLegacyConsumers(miningChainId, true);
        Document key = consumerKey(miningChainId, pipelineId);
        SourceOrder order = position.order();
        if (updateExistingConsumer(miningChainId, tableAckedBefore(key, table, order),
                sinkAckedUpdate(pipelineId, table, position))) {
            return;
        }
        Document atOrAfter = tableAckedAtOrAfter(key, table, order);
        boolean settled = order.seq() >= 0
                ? updateExistingConsumer(miningChainId, atOrAfter,
                        new Document("$max", new Document(PER_TABLE_RING_DONE + "." + table, order.seq())))
                : StoreIo.call(miningChainId, () -> consumers.find(atOrAfter)
                        .projection(Projections.include("_id")).first()) != null;
        if (settled) {
            return;
        }
        writeConsumer(miningChainId, session -> {
            Document held = consumers.find(session, key)
                    .projection(Projections.include(SINK_ACKED_BY_TABLE + "." + table)).first();
            SourceOrder last = held == null ? null : tableAckedFrom(held, table);
            Document write;
            if (last == null || position.order().compareTo(last) > 0) {
                write = sinkAckedUpdate(pipelineId, table, position);
            } else if (position.order().seq() >= 0) {
                write = new Document("$max",
                        new Document(PER_TABLE_RING_DONE + "." + table, position.order().seq()));
            } else {
                return;
            }
            write.append("$setOnInsert", consumerIdentity(miningChainId, pipelineId));
            consumers.updateOne(session, key, write, new UpdateOptions().upsert(true));
        });
    }

    /**
     * The fenced form of the table-aware advance: the same two conditional writes, made only while {@code fence}
     * is the live claim and the pipeline's cursor is bound to its run. The cursor is there by now - the run's
     * start created it - so there is nothing to create and nothing of the chain's own to hold.
     */
    @Override
    public boolean advanceSinkAcked(String miningChainId, String pipelineId, String table, ChainPosition position,
            WorkloadClaimFence fence) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(position.order(), "position order");
        migrateLegacyConsumers(miningChainId, true);
        Document key = consumerKey(miningChainId, pipelineId);
        SourceOrder order = position.order();
        return fencedConsumerWrite(miningChainId, pipelineId, fence, session -> {
            if (consumers.updateOne(session, tableAckedBefore(key, table, order),
                    sinkAckedUpdate(pipelineId, table, position)).getMatchedCount() == 0 && order.seq() >= 0) {
                consumers.updateOne(session, tableAckedAtOrAfter(key, table, order),
                        new Document("$max", new Document(PER_TABLE_RING_DONE + "." + table, order.seq())));
            }
            return Boolean.TRUE;
        }).isPresent();
    }

    /** The consumer-document field holding the current run's per-writer accounting. */
    static final String WRITER_RUN = "writerRun";

    @Override
    public void beginWriterRun(String miningChainId, String pipelineId, String runId,
            Map<String, List<String>> expectedWritersByTable) {
        beginWriterRun(miningChainId, pipelineId, runId, expectedWritersByTable, ConsumerProgressKind.LEGACY);
    }

    @Override
    public void beginWriterRun(String miningChainId, String consumerId, String runId,
            Map<String, List<String>> expectedWritersByTable, ConsumerProgressKind kind) {
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(kind, "kind");
        migrateLegacyConsumers(miningChainId, true);
        refuseLegacyProgress(miningChainId, consumerId, expectedWritersByTable);
        updateConsumer(miningChainId, consumerId, new Document("$set", new Document(WRITER_RUN,
                writerRunDocument(runId, expectedWritersByTable)).append(PROGRESS_KIND, kind.name())));
    }

    @Override
    public boolean beginWriterRun(String miningChainId, String pipelineId, String runId,
            Map<String, List<String>> expectedWritersByTable, WorkloadClaimFence fence) {
        return beginWriterRun(miningChainId, pipelineId, runId, expectedWritersByTable,
                ConsumerProgressKind.LEGACY, fence);
    }

    /**
     * Starts the run's accounting as the unfenced start does, and binds the consumer's later durable sink effects
     * on the chain to {@code fence}'s run - in one transaction with the proof that {@code fence} is the live claim,
     * and with the chain's lifecycle fence, because this is the write that may create the consumer's cursor. Once
     * a run a claim no longer names can prove nothing, it can neither start its accounting again nor take the
     * binding back from the run that holds it.
     */
    @Override
    public boolean beginWriterRun(String miningChainId, String consumerId, String runId,
            Map<String, List<String>> expectedWritersByTable, ConsumerProgressKind kind, WorkloadClaimFence fence) {
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(kind, "kind");
        Document bound = sinkAckFenceDocument(consumerId, fence);
        Document update = new Document("$set", new Document(WRITER_RUN,
                writerRunDocument(runId, expectedWritersByTable))
                .append(PROGRESS_KIND, kind.name())
                .append(SINK_ACK_FENCE, bound))
                .append("$setOnInsert", consumerIdentity(miningChainId, consumerId));
        Document key = consumerKey(miningChainId, consumerId);
        migrateLegacyConsumers(miningChainId, true);
        refuseLegacyProgress(miningChainId, consumerId, expectedWritersByTable);
        return StoreIo.call(miningChainId, () -> {
            try (ClientSession session = client.startSession()) {
                return session.withTransaction(() -> {
                    if (!provesClaim(session, fence)) {
                        return false;
                    }
                    UpdateResult rooted = collection.updateOne(session, new Document("_id", miningChainId),
                            new Document("$inc", new Document(CONSUMER_WRITE_REVISION, 1L)));
                    if (rooted.getMatchedCount() == 0) {
                        throw unseededChain(miningChainId);
                    }
                    consumers.updateOne(session, key, update, new UpdateOptions().upsert(true));
                    return true;
                });
            }
        });
    }

    /** A run's accounting as it starts: the writers each table is expected to reach, and no progress yet. */
    private static Document writerRunDocument(String runId, Map<String, List<String>> expectedWritersByTable) {
        Document expected = new Document();
        expectedWritersByTable.forEach((table, writers) -> expected.append(table, List.copyOf(writers)));
        return new Document("id", runId).append("expected", expected).append("progress", new Document());
    }

    /**
     * Refuses to start accounting for a consumer named for its source node while its pipeline still holds sink
     * progress recorded under the pipeline's own name on the chain - a confirmed position, a finished load, or a
     * writer's progress. That record cannot say which source node it was made for: carried over it may stand for
     * changes another node's sinks never wrote, and dropped it loses what a sink did write. Read cursors are not
     * progress any sink made, and do not count. Where the run plans more than one writer the progress could not be
     * split between them either, which is the code it answers with then.
     */
    private void refuseLegacyProgress(String miningChainId, String consumerId,
            Map<String, List<String>> expectedWritersByTable) {
        if (SrsConsumerId.sourceOf(consumerId).isEmpty()) {
            return;
        }
        String pipeline = SrsConsumerId.pipelineOf(consumerId);
        Document legacy = StoreIo.call(() -> consumers.find(consumerKey(miningChainId, pipeline)).first());
        if (legacy == null) {
            return;
        }
        boolean retained = sinkAckedFrom(legacy) != null || !snapshotCompletedFrom(legacy).isEmpty()
                || !confirmedByTable(legacy, pipeline).isEmpty()
                || writerRunOf(legacy).map(run -> run.progress().values().stream()
                        .anyMatch(byWriter -> !byWriter.isEmpty())).orElse(false);
        if (!retained) {
            return;
        }
        long writers = expectedWritersByTable.values().stream().flatMap(List::stream).distinct().count();
        throw new TapstateException(writers > 1 ? IoError.SINK_WRITER_PROGRESS_AMBIGUOUS
                : IoError.SRS_PROGRESS_UNPROVEN, Map.of("pipeline", pipeline), null);
    }

    /**
     * Raises the table's confirmed position where the one it holds is earlier, or where it holds none, and
     * otherwise only its place in the ring - each in one write of its own, the comparison in the write's own
     * filter, so no confirmation landing in between can split the two. It never creates the consumer's cursor:
     * the run's start did, and a cursor that is gone has gone with its chain or its consumer, and must not come
     * back.
     */
    @Override
    public void advanceTableConfirmed(String miningChainId, String consumerId, String table,
            ChainPosition confirmed) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(consumerId, "consumerId");
        Objects.requireNonNull(table, TABLE);
        Objects.requireNonNull(confirmed, POSITION);
        Objects.requireNonNull(confirmed.order(), POSITION_ORDER);
        migrateLegacyConsumers(miningChainId, true);
        Document key = consumerKey(miningChainId, consumerId);
        SourceOrder order = confirmed.order();
        if (!updateExistingConsumer(miningChainId, tableAckedBefore(key, table, order),
                tableConfirmedUpdate(table, confirmed)) && order.seq() >= 0) {
            updateExistingConsumer(miningChainId, tableAckedAtOrAfter(key, table, order), ringRaise(table, order));
        }
    }

    /** The fenced form of the table's confirmation: the same two conditional writes, while the fence holds. */
    @Override
    public boolean advanceTableConfirmed(String miningChainId, String consumerId, String table,
            ChainPosition confirmed, WorkloadClaimFence fence) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(consumerId, "consumerId");
        Objects.requireNonNull(table, TABLE);
        Objects.requireNonNull(confirmed, POSITION);
        Objects.requireNonNull(confirmed.order(), POSITION_ORDER);
        migrateLegacyConsumers(miningChainId, true);
        Document key = consumerKey(miningChainId, consumerId);
        SourceOrder order = confirmed.order();
        return fencedConsumerWrite(miningChainId, consumerId, fence, session -> {
            if (consumers.updateOne(session, tableAckedBefore(key, table, order),
                    tableConfirmedUpdate(table, confirmed)).getMatchedCount() == 0 && order.seq() >= 0) {
                consumers.updateOne(session, tableAckedAtOrAfter(key, table, order), ringRaise(table, order));
            }
            return Boolean.TRUE;
        }).isPresent();
    }

    /** One write raising the consumer's acked position, the comparison with the one it holds in its filter. */
    @Override
    public void raiseSinkAcked(String miningChainId, String consumerId, ChainPosition position) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(consumerId, "consumerId");
        Objects.requireNonNull(position, POSITION);
        Objects.requireNonNull(position.order(), POSITION_ORDER);
        migrateLegacyConsumers(miningChainId, true);
        updateExistingConsumer(miningChainId,
                ackedBefore(consumerKey(miningChainId, consumerId), position.order()),
                sinkAckedUpdate(consumerId, position));
    }

    /** The fenced form of the raise, while the fence holds. */
    @Override
    public boolean raiseSinkAcked(String miningChainId, String consumerId, ChainPosition position,
            WorkloadClaimFence fence) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(consumerId, "consumerId");
        Objects.requireNonNull(position, POSITION);
        Objects.requireNonNull(position.order(), POSITION_ORDER);
        migrateLegacyConsumers(miningChainId, true);
        Document filter = ackedBefore(consumerKey(miningChainId, consumerId), position.order());
        Document update = sinkAckedUpdate(consumerId, position);
        return fencedConsumerWrite(miningChainId, consumerId, fence, session -> {
            consumers.updateOne(session, filter, update);
            return Boolean.TRUE;
        }).isPresent();
    }

    /**
     * Works the direct channel's progress out from what is stored, in one transaction with the chain's own record:
     * the batches it completes, and the checkpoint they move, are read and written as one act, so two confirmations
     * settling at once cannot both take the same batches off, nor one undo the other's checkpoint. A consumer that
     * is not a direct channel's, or one whose chain has opened a generation its stream does not belong to, is
     * left as it is.
     */
    @Override
    public void settleDirectBatches(String miningChainId, String consumerId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(consumerId, "consumerId");
        migrateLegacyConsumers(miningChainId, true);
        Document key = consumerKey(miningChainId, consumerId);
        StoreIo.run(miningChainId, () -> {
            try (ClientSession session = client.startSession()) {
                session.withTransaction(() -> {
                    Document consumer = consumers.find(session, key).first();
                    if (consumer != null
                            && progressKind(consumer, consumerId) == ConsumerProgressKind.DIRECT_SOURCE) {
                        settleDirect(session, miningChainId, consumerId, key, consumer);
                    }
                    return null;
                });
            }
        });
    }

    private void settleDirect(ClientSession session, String chain, String consumerId, Document key,
            Document consumer) {
        Map<String, ChainPosition> confirmed = confirmedByTable(consumer, consumerId);
        if (!consumer.containsKey("directEpoch")) {
            // No batch recorded: the tables' own positions are all there is, and their lowest is a prefix of the
            // channel's one source order once every table of the run has one.
            ChainPosition lowest = null;
            for (String table : writerRunOf(consumer).map(run -> run.expected().keySet()).orElse(Set.of())) {
                ChainPosition position = confirmed.get(table);
                if (position == null) {
                    return;
                }
                if (lowest == null || position.order().compareTo(lowest.order()) < 0) {
                    lowest = position;
                }
            }
            if (lowest != null) {
                consumers.updateOne(session, ackedBefore(key, lowest.order()), sinkAckedUpdate(consumerId, lowest));
            }
            return;
        }
        long activeEpoch = readEpoch(consumer, "directEpoch");
        Document root = collection.find(session, new Document("_id", chain))
                .projection(Projections.include("epoch")).first();
        if (root == null || readEpoch(root, "epoch") != activeEpoch) {
            return;
        }
        List<Document> batches = directBatches(consumer, consumerId);
        int completed = 0;
        ChainPosition candidate = null;
        for (Document batch : batches) {
            long epoch = readEpoch(batch, "epoch");
            Document targets = batch.get("targets", Document.class);
            boolean settled = epoch == activeEpoch && targets != null;
            if (settled) {
                for (Map.Entry<String, Object> target : targets.entrySet()) {
                    ChainPosition ack = confirmed.get(target.getKey());
                    if (!(target.getValue() instanceof Number sequence) || ack == null
                            || ack.order().epoch() != epoch || ack.order().seq() < sequence.longValue()) {
                        settled = false;
                        break;
                    }
                }
            }
            if (!settled) {
                break;
            }
            candidate = new ChainPosition(new SourceOrder(epoch, readEpoch(batch, "seq")), batch.getString("token"));
            completed++;
        }
        if (candidate == null) {
            return;
        }
        Document update = sinkAckedUpdate(consumerId, candidate);
        update.get("$set", Document.class).append("directBatches",
                new ArrayList<>(batches.subList(completed, batches.size())));
        consumers.updateOne(session, key, update);
        advanceDirectCheckpoint(session, chain, candidate);
    }

    @Override
    public void beginDirectCapture(String chain, String consumerId, long epoch, String anchor) {
        mutateConsumerInSession(chain, consumerId, (session, consumer) -> {
            requireDirectEpoch(session, chain, epoch);
            if (consumer.containsKey("directEpoch")) {
                long priorEpoch = readEpoch(consumer, "directEpoch");
                if (priorEpoch > epoch) {
                    throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
                }
                if (priorEpoch == epoch) {
                    // Capture first registers its stream, then reports its initial database boundary.
                    // Repeating either call must never discard batches already awaiting their writers.
                    if (anchor == null || consumer.containsKey("directAnchor")) {
                        return false;
                    }
                    if (!directBatches(consumer, consumerId).isEmpty()) {
                        throw new TapstateException(IoError.SRS_PROGRESS_UNPROVEN,
                                Map.of("pipeline", SrsConsumerId.pipelineOf(consumerId)), null);
                    }
                } else {
                    consumer.put("directBatches", new ArrayList<Document>());
                    consumer.remove("directAnchor");
                }
            } else {
                consumer.put("directBatches", new ArrayList<Document>());
            }
            consumer.put(PROGRESS_KIND, ConsumerProgressKind.DIRECT_SOURCE.name());
            consumer.put("directEpoch", epoch);
            if (anchor != null) {
                ChainPosition initial = new ChainPosition(SourceOrder.snapshotRow(epoch), anchor);
                writePosition(consumer, initial);
                consumer.put("directAnchor", anchor);
                advanceDirectCheckpoint(session, chain, initial);
            }
            return true;
        });
    }

    @Override
    public void recordDirectBatch(String chain, String consumerId, ChainPosition position,
            Map<String, Long> targets) {
        Objects.requireNonNull(position.order(), POSITION_ORDER);
        if (position.token() == null) {
            return;
        }
        mutateConsumerInSession(chain, consumerId, (session, consumer) -> {
            requireDirectEpoch(session, chain, position.order().epoch());
            if (readEpoch(consumer, "directEpoch") != position.order().epoch()) {
                throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
            }
            List<Document> pending = new ArrayList<>(directBatches(consumer, consumerId));
            Document batch = new Document("epoch", position.order().epoch())
                    .append("seq", position.order().seq()).append("token", position.token())
                    .append("targets", new Document(targets));
            pending.add(batch);
            consumer.put("directBatches", pending);
            return true;
        });
    }

    /** Producer metadata is valid only for the generation currently owned by this capture chain. */
    private void requireDirectEpoch(ClientSession session, String chain, long epoch) {
        Document root = collection.find(session, new Document("_id", chain))
                .projection(Projections.include("epoch")).first();
        if (root == null || readEpoch(root, "epoch") != epoch) {
            throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
        }
    }

    /**
     * Mutates one complete consumer document - one the mutation may create - in a transaction with the chain's
     * lifecycle fence. Only a direct channel's stream registrations take this path: they are a function of several
     * of the document's values at once, and they happen once a generation or once a source batch, never once a
     * confirmation.
     */
    private void mutateConsumerInSession(String miningChainId, String consumerId,
            SessionConsumerDocumentMutation mutation) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(consumerId, "consumerId");
        migrateLegacyConsumers(miningChainId, true);
        writeConsumer(miningChainId, session -> {
            Document key = consumerKey(miningChainId, consumerId);
            Document consumer = consumers.find(session, key).first();
            if (consumer == null) {
                consumer = new Document(key);
                consumer.putAll(consumerIdentity(miningChainId, consumerId));
            }
            if (mutation.apply(session, consumer)) {
                consumers.replaceOne(session, key, consumer, new ReplaceOptions().upsert(true));
                // A batch can be recorded after the changes it carried were confirmed, so it settles here as well.
                if (progressKind(consumer, consumerId) == ConsumerProgressKind.DIRECT_SOURCE) {
                    settleDirect(session, miningChainId, consumerId, key, consumer);
                }
            }
        });
    }

    private void advanceDirectCheckpoint(ClientSession session, String chain, ChainPosition candidate) {
        Document filter = sourceReadAdvanceFilter(chain, candidate.order());
        filter.append("epoch", candidate.order().epoch());
        collection.updateOne(session, filter, new Document("$set",
                sourceReadFields(candidate, Instant.now(clock)).append("sourceReadDurable", false)));
    }

    private static List<Document> directBatches(Document consumer, String consumerId) {
        Object raw = consumer.get("directBatches");
        if (raw == null) {
            return List.of();
        }
        if (!(raw instanceof List<?> entries)) {
            throw unreadableConsumer(consumerId, "directBatches");
        }
        List<Document> batches = new ArrayList<>(entries.size());
        for (Object entry : entries) {
            if (!(entry instanceof Document document)
                    || !(document.get("epoch") instanceof Number)
                    || !(document.get("seq") instanceof Number)
                    || !(document.get("token") instanceof String)
                    || !(document.get("targets") instanceof Document targets)) {
                throw unreadableConsumer(consumerId, "directBatches");
            }
            long boundary = readEpoch(document, "seq");
            for (Object target : targets.values()) {
                if (!(target instanceof Number sequence)
                        || sequence.longValue() < 0 || sequence.longValue() > boundary) {
                    throw unreadableConsumer(consumerId, "directBatches");
                }
            }
            batches.add(document);
        }
        return batches;
    }

    /** The table's confirmed position, and its ring place raised to the position's sequence, in one update. */
    private static Document tableConfirmedUpdate(String table, ChainPosition confirmed) {
        Document update = new Document("$set",
                new Document(SINK_ACKED_BY_TABLE + "." + table, positionDocument(confirmed)));
        if (confirmed.order().seq() >= 0) {
            update.putAll(ringRaise(table, confirmed.order()));
        }
        return update;
    }

    /** A raise of the table's ring place to {@code order}'s sequence. */
    private static Document ringRaise(String table, SourceOrder order) {
        return new Document("$max", new Document(PER_TABLE_RING_DONE + "." + table, order.seq()));
    }

    /**
     * The consumer at {@code key}, where its acked position is earlier than {@code order}, or where it holds none
     * that can be compared - ranked by generation first, as an order is.
     */
    static Document ackedBefore(Document key, SourceOrder order) {
        return new Document(key).append("$or", List.of(
                new Document("sinkAckedEpoch", new Document("$not", new Document("$type", "number"))),
                new Document("sinkAckedSeq", new Document("$not", new Document("$type", "number"))),
                new Document("sinkAckedEpoch", new Document("$lt", order.epoch())),
                new Document("sinkAckedEpoch", order.epoch()).append("sinkAckedSeq",
                        new Document("$lt", order.seq()))));
    }

    @Override
    public Optional<WriterRun> advanceWriter(String miningChainId, String pipelineId, String runId,
            String writerId, String table, WriterProgress progress) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(writerId, "writerId");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(progress, "progress");
        migrateLegacyConsumers(miningChainId, true);
        Document filter = writerRunFilter(miningChainId, pipelineId, runId);
        Document update = writerProgressUpdate(writerId, table, progress);
        // One write of its own, like every write to a document that is already there: a run's accounting is
        // only ever advanced here, never created, so a document that is not there, or carries another run, is
        // left as it is and answers nothing.
        Document after = StoreIo.call(miningChainId,
                () -> consumers.findOneAndUpdate(filter, update, WRITER_RUN_AFTER));
        return after == null ? Optional.empty() : writerRunOf(after);
    }

    /** The fenced form of the writer's report: the same write, while {@code fence} proves the run current. */
    @Override
    public Optional<WriterRun> advanceWriter(String miningChainId, String pipelineId, String runId,
            String writerId, String table, WriterProgress progress, WorkloadClaimFence fence) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(writerId, "writerId");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(progress, "progress");
        migrateLegacyConsumers(miningChainId, true);
        Document filter = writerRunFilter(miningChainId, pipelineId, runId);
        Document update = writerProgressUpdate(writerId, table, progress);
        return fencedConsumerWrite(miningChainId, pipelineId, fence,
                session -> consumers.findOneAndUpdate(session, filter, update, WRITER_RUN_AFTER))
                .flatMap(MongoSrsMetaStore::writerRunOf);
    }

    /** What a writer's report answers with: its run's accounting as it stands after the write. */
    private static final FindOneAndUpdateOptions WRITER_RUN_AFTER = new FindOneAndUpdateOptions()
            .returnDocument(ReturnDocument.AFTER)
            .projection(Projections.include(WRITER_RUN, "snapshotEpoch", "cdcStartPosition"));

    /** The pipeline's cursor on the chain, while it carries {@code runId}'s accounting. */
    private static Document writerRunFilter(String miningChainId, String pipelineId, String runId) {
        return new Document(consumerKey(miningChainId, pipelineId)).append(WRITER_RUN + ".id", runId);
    }

    /**
     * The replacement of one writer's entry in its run's accounting. The writer's key is encoded because a writer
     * id names its vertex, and a vertex name may hold the dot a field path reads as a step into a nested
     * document; the id itself travels inside the entry.
     */
    private static Document writerProgressUpdate(String writerId, String table, WriterProgress progress) {
        String path = WRITER_RUN + ".progress." + table + "." + writerKey(writerId);
        Document entry = new Document("writer", writerId)
                .append("durableEpoch", progress.durableThrough().epoch())
                .append("durableSeq", progress.durableThrough().seq());
        if (progress.lastTokened() != null) {
            entry.append("tokenEpoch", progress.lastTokened().order().epoch())
                    .append("tokenSeq", progress.lastTokened().order().seq())
                    .append("token", progress.lastTokened().token());
        }
        return new Document("$set", new Document(path, entry));
    }

    @Override
    public Optional<WriterRun> writerRun(String miningChainId, String pipelineId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Document consumer = StoreIo.call(() -> consumers.find(consumerKey(miningChainId, pipelineId))
                .projection(Projections.include(WRITER_RUN, "snapshotEpoch", "cdcStartPosition")).first());
        return consumer == null ? Optional.empty() : writerRunOf(consumer);
    }

    /** A writer id as a field name: its UTF-8 bytes in URL-safe base64, which holds no dot or dollar. */
    static String writerKey(String writerId) {
        return java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString(writerId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /** The run accounting a consumer document carries, or empty where it carries none. */
    private static Optional<WriterRun> writerRunOf(Document consumer) {
        if (!(consumer.get(WRITER_RUN) instanceof Document run)) {
            return Optional.empty();
        }
        Map<String, List<String>> expected = new LinkedHashMap<>();
        if (run.get("expected") instanceof Document byTable) {
            for (Map.Entry<String, Object> table : byTable.entrySet()) {
                List<String> writers = new ArrayList<>();
                if (table.getValue() instanceof List<?> listed) {
                    listed.forEach(writer -> writers.add(String.valueOf(writer)));
                }
                expected.put(table.getKey(), writers);
            }
        }
        Map<String, Map<String, WriterProgress>> progress = new LinkedHashMap<>();
        if (run.get("progress") instanceof Document byTable) {
            for (Map.Entry<String, Object> table : byTable.entrySet()) {
                Map<String, WriterProgress> byWriter = new LinkedHashMap<>();
                if (table.getValue() instanceof Document entries) {
                    for (Object value : entries.values()) {
                        if (value instanceof Document entry) {
                            ChainPosition tokened = entry.getString("token") == null ? null : new ChainPosition(
                                    new SourceOrder(entry.getLong("tokenEpoch"), entry.getLong("tokenSeq")),
                                    entry.getString("token"));
                            byWriter.put(entry.getString("writer"), new WriterProgress(
                                    new SourceOrder(entry.getLong("durableEpoch"), entry.getLong("durableSeq")),
                                    tokened));
                        }
                    }
                }
                progress.put(table.getKey(), byWriter);
            }
        }
        Long snapshotEpoch = consumer.getString("cdcStartPosition") == null ? null
                : readEpoch(consumer, "snapshotEpoch");
        return Optional.of(new WriterRun(run.getString("id"), expected, progress, snapshotEpoch));
    }

    @Override
    public void startRingAfter(String miningChainId, String pipelineId, String table, long seq) {
        Objects.requireNonNull(table, "table");
        // Read, then write only when there is no arrival yet: a pipeline arrives on a ring from one member
        // at a time, so nothing races this, and a raise over a place the pipeline already has would carry it
        // past changes it has not received.
        if (ringDoneThrough(miningChainId, pipelineId).containsKey(table)) {
            return;
        }
        // The completed place and the read cursor are one arrival fact. Publishing only the former leaves
        // the consumer absent from the write-side headroom minimum until a later registration write lands;
        // enough concurrent changes can then overwrite the first changes written after this arrival.
        updateConsumer(miningChainId, pipelineId, consumerArrivalUpdate(table, seq));
    }

    /** The one update that publishes both halves of a consumer's first arrival on a table ring. */
    static Document consumerArrivalUpdate(String table, long seq) {
        Objects.requireNonNull(table, "table");
        return new Document("$max", new Document(PER_TABLE_RING_DONE + "." + table, seq)
                .append("perTableSeq." + table, seq));
    }

    @Override
    public Map<String, Long> ringDoneThrough(String miningChainId, String pipelineId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Document consumer = StoreIo.call(() -> consumers.find(consumerKey(miningChainId, pipelineId))
                .projection(Projections.include(PER_TABLE_RING_DONE, SINK_ACKED_BY_TABLE, PROGRESS_KIND)).first());
        if (consumer == null) {
            return Map.of();
        }
        Map<String, Long> seqs = ringDoneFromDocument(consumer, pipelineId);
        if (progressKind(consumer, pipelineId) == ConsumerProgressKind.SRS) {
            for (Map.Entry<String, ChainPosition> entry : confirmedByTable(consumer, pipelineId).entrySet()) {
                SourceOrder order = entry.getValue().order();
                if (order == null || order.epoch() < 1L) {
                    throw unreadableConsumer(pipelineId, SINK_ACKED_BY_TABLE);
                }
                if (order.seq() >= 0L) {
                    // This table's actual confirmation remains proof when its redundant marker is absent.
                    seqs.merge(entry.getKey(), order.seq(), Math::max);
                }
            }
        }
        return Map.copyOf(seqs);
    }

    private static Map<String, Long> ringDoneFromDocument(Document consumer, String pipelineId) {
        Map<String, Long> seqs = new LinkedHashMap<>();
        if (!consumer.containsKey(PER_TABLE_RING_DONE)) {
            return seqs;
        }
        if (!(consumer.get(PER_TABLE_RING_DONE) instanceof Document perTable)) {
            throw unreadableConsumer(pipelineId, PER_TABLE_RING_DONE);
        }
        for (Map.Entry<String, Object> entry : perTable.entrySet()) {
            Object raw = entry.getValue();
            if (!(raw instanceof Long || raw instanceof Integer) || ((Number) raw).longValue() < -1L) {
                throw unreadableConsumer(pipelineId, PER_TABLE_RING_DONE);
            }
            seqs.put(entry.getKey(), ((Number) raw).longValue());
        }
        return seqs;
    }

    /**
     * The path-scoped update advancing one consumer document's read cursor for one table. It raises only
     * {@code perTableSeq.<table>}, so registration at -1 and a slower publisher cannot lower an advanced
     * cursor or touch the sink-acked position. The L1 stream name is a bare identifier, so the dotted path
     * addresses exactly one field. A deep {@code $max} creates the cursor map when none exists yet.
     */
    static Document consumerReadSeqUpdate(String pipelineId, String table, long lastReadSeq) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(table, "table");
        return new Document("$max", new Document("perTableSeq." + table, lastReadSeq));
    }

    /**
     * The path-scoped update that advances one consumer document's durable sink-acked position: a
     * {@code $set} on the token and the two fields carrying the order it sat at, so the reader's per-table
     * cursor in that document is left untouched. An upsert lets a sink ack before the consumer has any
     * other cursor state.
     *
     * <p>The three fields move together in one update. A token stored without its order can no longer be
     * ranked against anything, and an order stored without its token is nothing a read can resume from;
     * either alone would be a record no later comparison can use.
     *
     * <p>A position carrying no token clears the stored one rather than leaving it, and that is the same
     * rule rather than an exception to it. A source names a position for a run of changes when it has one,
     * so an ack with none is an order that no token belongs to; leaving the token an earlier and lower
     * position stored pairs this order with it, and the pair is read back as one position. The source-read
     * advance both ranks and writes that pair down, so the chain's offset moves to this order carrying a
     * token from beneath it -- and a real token arriving in between is then refused as a rewind against an
     * order it never reached. Cleared, the position reads back as ordered and tokenless, which that advance
     * already declines to write down, leaving the offset where it stands.
     */
    static Document sinkAckedUpdate(String pipelineId, ChainPosition position) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(position.order(), "position order");
        Document fields = new Document("sinkAckedEpoch", position.order().epoch())
                .append("sinkAckedSeq", position.order().seq());
        Document update = new Document("$set", fields);
        if (position.token() != null) {
            fields.append("sinkAckedSrcpos", position.token());
        } else {
            update.append("$unset", new Document("sinkAckedSrcpos", ""));
        }
        return update;
    }

    /**
     * The same update, raising {@code perTableRingDone.<table>} to the ring sequence the order carries in
     * the same write, so the per-table record can never run ahead of the chain position it came with. A
     * {@code $max} rather than a set: two members confirming at once both write, and the record only ever
     * moves forward. A snapshot row sits at a reserved sequence below every change and is no place in any
     * ring, so it raises nothing. The order is also kept as the table's last acked one, which the next ack of
     * the table is compared with.
     */
    static Document sinkAckedUpdate(String pipelineId, String table, ChainPosition position) {
        Objects.requireNonNull(table, "table");
        Document update = sinkAckedUpdate(pipelineId, position);
        update.get("$set", Document.class).append(SINK_ACKED_BY_TABLE + "." + table, positionDocument(position));
        if (position.order().seq() >= 0) {
            update.append("$max", new Document(PER_TABLE_RING_DONE + "." + table, position.order().seq()));
        }
        return update;
    }

    /** A position in the shape the acked position itself is stored in: its order's two numbers, and its token. */
    private static Document positionDocument(ChainPosition position) {
        Document stored = new Document();
        writePosition(stored, position);
        return stored;
    }

    private static void writePosition(Document document, ChainPosition position) {
        document.put("sinkAckedEpoch", position.order().epoch());
        document.put("sinkAckedSeq", position.order().seq());
        if (position.token() == null) {
            document.remove("sinkAckedSrcpos");
        } else {
            document.put("sinkAckedSrcpos", position.token());
        }
    }

    /**
     * The consumer at {@code key}, where the last acked position written for {@code table} is earlier than
     * {@code order}, or where there is none it can be compared with - read as {@link #tableAckedFrom} reads
     * one: a generation and a ring sequence, both numbers. Ranked by generation first, as an order is.
     */
    static Document tableAckedBefore(Document key, String table, SourceOrder order) {
        String epoch = SINK_ACKED_BY_TABLE + "." + table + ".sinkAckedEpoch";
        String seq = SINK_ACKED_BY_TABLE + "." + table + ".sinkAckedSeq";
        return new Document(key).append("$or", List.of(
                new Document(epoch, new Document("$not", new Document("$type", "number"))),
                new Document(seq, new Document("$not", new Document("$type", "number"))),
                new Document(epoch, new Document("$lt", order.epoch())),
                new Document(epoch, order.epoch()).append(seq, new Document("$lt", order.seq()))));
    }

    /** The rest of the consumer at {@code key}: a last acked position for {@code table} at or after {@code order}. */
    static Document tableAckedAtOrAfter(Document key, String table, SourceOrder order) {
        String epoch = SINK_ACKED_BY_TABLE + "." + table + ".sinkAckedEpoch";
        String seq = SINK_ACKED_BY_TABLE + "." + table + ".sinkAckedSeq";
        return new Document(key)
                .append(seq, new Document("$type", "number"))
                .append("$or", List.of(
                        new Document(epoch, new Document("$gt", order.epoch())),
                        new Document(epoch, order.epoch()).append(seq, new Document("$gte", order.seq()))));
    }

    /** The order of the last acked position written for {@code table}, or null while none has been. */
    private static SourceOrder tableAckedFrom(Document document, String table) {
        Document byTable = document.get(SINK_ACKED_BY_TABLE, Document.class);
        Document last = byTable == null ? null : byTable.get(table, Document.class);
        ChainPosition position = last == null ? null : sinkAckedFrom(last);
        return position == null ? null : position.order();
    }

    private static TapstateException unreadableConsumer(String consumerId, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", consumerId, "field", field), null);
    }

    /** Returns a nested document, or null where there is none, rejecting a malformed stored value. */
    private static Document nestedDocument(Document parent, String field, String consumerId) {
        Object raw = parent.get(field);
        if (raw instanceof Document document) {
            return document;
        }
        if (raw != null) {
            throw unreadableConsumer(consumerId, field);
        }
        return null;
    }

    @Override
    public void setCdcStart(
            String miningChainId, String pipelineId, String cdcStartPosition, long snapshotEpoch) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(cdcStartPosition, "cdcStartPosition");
        if (snapshotEpoch < 0) {
            throw new IllegalArgumentException("snapshotEpoch must not be negative, got " + snapshotEpoch);
        }
        // One update, both fields: a resumed snapshot reads them together, so a state where the seam
        // position is stored without the generation it belongs to must not be reachable.
        updateConsumer(miningChainId, pipelineId,
                new Document("$set", new Document("cdcStartPosition", cdcStartPosition)
                        .append("snapshotEpoch", snapshotEpoch)));
    }

    @Override
    public long openEpoch(String miningChainId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        // An atomic increment read back after the write: two members opening the same chain must take two
        // different generations, so the counter is advanced by the store rather than read, added to and
        // written back. It touches only epoch, leaving every pinned pipeline snapshot generation where it is.
        Document updated = writeChainWithConsumerMigration(miningChainId, () -> collection.findOneAndUpdate(
                new Document("_id", miningChainId),
                new Document("$inc", new Document("epoch", 1L)),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)));
        if (updated == null) {
            throw unseededChain(miningChainId);
        }
        return readEpoch(updated, "epoch");
    }

    /**
     * Appends a version to the chain's schema history and cuts the retained history back to its budget,
     * in one atomic update.
     *
     * <p>The cut is what keeps the append landable however far the history has already grown, and it has
     * to be part of the same write. A trim on its own would be a second write, and between the two the
     * record would still carry an array the endpoint refuses to grow, so a schema change arriving then
     * would be the one that cannot be recorded. Cutting as part of the append means the entry being
     * appended is written or the update does not happen at all.
     *
     * <p>What it drops is the oldest entries, whole. An entry is never rewritten to make room: versions
     * are what a consumer resolves the schema in force at a change against, and a version rewritten to
     * something smaller would resolve to a schema its table never had, which is worse than a version
     * missing.
     */
    @Override
    public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
        Objects.requireNonNull(version, "version");
        updatePipeline(miningChainId, List.of(new Document("$set",
                new Document("schemaHistory", historyWithinBudget(schemaToDocument(version))))));
    }

    /**
     * The expression that appends one stored entry to the stored history and cuts the array back to the
     * newest entries the budget holds. Runs inside the update, so what it reads and what it writes are one
     * atomic act and two appends racing on one chain cannot lose each other's entry.
     *
     * <p>The cut walks the array backwards, from the entry just appended to the oldest one, counting how
     * many of the newest entries the budget holds; what it writes back is that many entries off the end.
     * The entry just appended is counted whatever it weighs — the write exists to record it — and the first
     * entry that does not fit closes the window, so what is retained is a suffix of the history: the newest
     * versions, contiguous, rather than whichever older entries happened to be small enough to squeeze in.
     *
     * <p>Only the count travels through the walk, never the entries. An accumulator that carried the kept
     * entries would copy that array on every step and so cost the square of what it retains, and what it
     * retains is a byte budget rather than a count: a narrow table's entries are tens of bytes, so
     * thousands of them fit, and the cost of a single append was measured at hundreds of milliseconds
     * there. A tail slice of a counted length is also already oldest-first, which is the order the record
     * stores.
     */
    private static Document historyWithinBudget(Document newEntry) {
        // A missing history reads as an empty one: this is the only write that can record a schema change,
        // so a record it refused to grow would be a chain that can no longer say its schema moved.
        Document appended = new Document("$concatArrays", List.of(
                new Document("$ifNull", List.of("$schemaHistory", List.of())),
                List.of(new Document("$literal", newEntry))));
        Document cut = new Document("$reduce",
                new Document("input", new Document("$reverseArray", "$$all"))
                        .append("initialValue", new Document("count", 0)
                                .append("used", 0L)
                                .append("closed", false))
                        .append("in", countOfNewestWithinBudget()));
        // The walk reads `$$all`, so it is bound around it, and the count it arrives at is taken off the
        // end of that same array — the newest entries, in the order they arrived.
        return new Document("$let", new Document("vars", new Document("all", appended))
                .append("in", new Document("$let", new Document("vars", new Document("cut", cut))
                        .append("in", new Document("$slice", List.of("$$all",
                                new Document("$subtract", List.of(0, "$$cut.count"))))))));
    }

    /**
     * One step of that walk: the accumulator carries how many of the newest entries are kept, what they
     * weigh between them, and whether the window has closed. Once it has closed the step reads nothing
     * further — the entries the walk has already decided to drop are never sized.
     *
     * <p>An element that is not a document is charged more than the whole budget, which closes the window
     * on it and on everything older. It is not a version — no consumer could resolve a schema against it —
     * and it must not be sized either: asking for the size of a string aborts the update, and asking for
     * the size of a null answers null, which compares as under any budget and would leave the history
     * growing unbounded again, silently, which is the state this bound exists to end.
     */
    private static Document countOfNewestWithinBudget() {
        Document isDocument = new Document("$eq", List.of(new Document("$type", "$$this"), "object"));
        // Sized as itself where it is a document and as an empty one where it is not, so that what reaches
        // the size operator is a document whether or not the branch that uses the answer is taken.
        Document sizeable = new Document("$cond",
                List.of(isDocument, "$$this", new Document("$literal", new Document())));
        Document bytes = new Document("$cond", List.of(isDocument,
                new Document("$add",
                        List.of(new Document("$bsonSize", "$$sizeable"), HISTORY_ENTRY_OVERHEAD_BYTES)),
                SCHEMA_HISTORY_BUDGET_BYTES + 1));
        // Dropping one drops every older entry with it, which is what makes the survivor a suffix.
        Document dropped = new Document("count", "$$value.count")
                .append("used", "$$value.used")
                .append("closed", true);
        Document keep = new Document("$cond", List.of(
                new Document("$or", List.of(
                        new Document("$eq", List.of("$$value.count", 0)),
                        new Document("$and", List.of(
                                new Document("$eq", List.of("$$value.closed", false)),
                                new Document("$lte", List.of(
                                        new Document("$add", List.of("$$value.used", "$$bytes")),
                                        SCHEMA_HISTORY_BUDGET_BYTES)))))),
                new Document("count", new Document("$add", List.of("$$value.count", 1)))
                        .append("used", new Document("$add", List.of("$$value.used", "$$bytes")))
                        .append("closed", false),
                dropped));
        return new Document("$cond", List.of("$$value.closed", dropped,
                new Document("$let", new Document("vars", new Document("sizeable", sizeable))
                        .append("in", new Document("$let", new Document("vars", new Document("bytes", bytes))
                                .append("in", keep))))));
    }

    @Override
    public void markSnapshotComplete(String miningChainId, String pipelineId, String table) {
        updateConsumer(miningChainId, pipelineId, snapshotCompleteUpdate(pipelineId, table));
    }

    /** The fenced form of the snapshot mark, under the same condition as the fenced advance. */
    @Override
    public boolean markSnapshotComplete(String miningChainId, String pipelineId, String table,
            WorkloadClaimFence fence) {
        Document update = snapshotCompleteUpdate(pipelineId, table);
        migrateLegacyConsumers(miningChainId, true);
        Document key = consumerKey(miningChainId, pipelineId);
        return fencedConsumerWrite(miningChainId, pipelineId, fence,
                session -> consumers.updateOne(session, key, update)).isPresent();
    }

    /**
     * The update that marks one table's snapshot drained in one consumer document: an {@code $addToSet}
     * on {@code snapshotCompletedTables}. A set add, not a push — the mark answers "has this table landed
     * in this pipeline's target?", so re-marking a table must be a no-op rather than a duplicate entry.
     *
     * <p>The containing document scopes the mark to the pipeline. Recording it against the chain instead
     * is what let a pipeline new to a shared chain read another pipeline's answer and skip a load it had
     * never done. An upsert creates the consumer entry when the pipeline has none and touches nothing else.
     */
    static Document snapshotCompleteUpdate(String pipelineId, String table) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(table, "table");
        return new Document("$addToSet", new Document("snapshotCompletedTables", table));
    }

    @Override
    public List<String> miningChainIdsWithConsumer(String pipelineId) {
        Objects.requireNonNull(pipelineId, PIPELINE_ID);
        // Embedded records predate the owner field. Inspect their keys without reconstructing cursors,
        // so an unreadable cursor cannot prevent a departing pipeline from detaching.
        LinkedHashSet<String> chains = new LinkedHashSet<>();
        List<Document> embedded = StoreIo.call(() -> collection
                .find(new Document("consumerOffsets", new Document("$type", "object")))
                .projection(Projections.include("_id", "consumerOffsets"))
                .into(new ArrayList<>()));
        for (Document root : embedded) {
            Document cursors = root.get("consumerOffsets", Document.class);
            if (cursors.keySet().stream().anyMatch(id -> consumerBelongsTo(id, pipelineId))) {
                chains.add(root.getString("_id"));
            }
        }
        StoreIo.call(() -> consumers.find(new Document("$or", List.of(
                        new Document(PIPELINE_ID, pipelineId), new Document("ownerPipelineId", pipelineId))))
                .projection(Projections.include("miningChainId"))
                .map(document -> document.getString("miningChainId"))
                .into(chains));
        return List.copyOf(chains);
    }

    private static boolean consumerBelongsTo(String consumerId, String pipelineId) {
        try {
            return consumerId.equals(pipelineId) || SrsConsumerId.belongsTo(consumerId, pipelineId);
        } catch (IllegalArgumentException malformedId) {
            // A malformed unrelated key has no proven owner and cannot block cleanup of known records.
            return false;
        }
    }

    @Override
    public void dropChain(String miningChainId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        // The root and its split cursors are one lifecycle fact, even though they occupy two collections.
        // Deleting both in one transaction leaves no point at which the id can be seeded again while the
        // old cleanup can still reach its new cursors. The driver may retry the body; both deletes are
        // idempotent, so repeating them preserves the same end state.
        StoreIo.run(miningChainId, () -> {
            try (ClientSession session = client.startSession()) {
                session.withTransaction(() -> {
                    collection.deleteOne(session, new Document("_id", miningChainId));
                    consumers.deleteMany(session, consumersOfChain(miningChainId));
                    return null;
                });
            }
        });
    }

    @Override
    public void detachConsumer(String miningChainId, String pipelineId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        // A detach is idempotent, so an absent chain is already the requested end state. Migration still
        // runs when the chain exists, preserving every other legacy cursor before this one is removed.
        // Its transaction also serializes any older migration snapshot before the delete, so no durable
        // marker has to remain merely to keep a stale copier from recreating this cursor.
        migrateLegacyConsumers(miningChainId, false);
        Document deletion = SrsConsumerId.sourceOf(pipelineId).isPresent()
                ? consumerKey(miningChainId, pipelineId)
                : new Document("miningChainId", miningChainId).append("$or", List.of(
                        new Document(PIPELINE_ID, pipelineId), new Document("ownerPipelineId", pipelineId)));
        StoreIo.run(() -> consumers.deleteMany(deletion));
    }

    /**
     * The membership test for one consumer: a chain matches when it carries a cursor at
     * {@code consumerOffsets.<pipelineId>}. The pipeline id is a resource id the grammar forbids a dot in,
     * so the dotted path addresses exactly one field and cannot reach into a neighbouring consumer's.
     */
    static Document consumerPresenceFilter(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        return new Document("consumerOffsets." + pipelineId, new Document("$exists", true));
    }

    /**
     * Applies an update written as a pipeline — the form an update takes when what it writes is a function
     * of what the record already holds, which no single update operator expresses. Still one atomic act on
     * one chain document.
     */
    private void updatePipeline(String miningChainId, List<Document> pipeline) {
        applyToSeeded(miningChainId,
                () -> collection.updateOne(new Document("_id", miningChainId), pipeline));
    }

    /**
     * Checks the result of a chain update. A zero matched count means no document carried the id — the
     * chain was never seeded, a caller ordering error surfaced bare (not laundered into an io code). The
     * chain id is handed to the translation so that a size refusal names the record it was for.
     */
    private void applyToSeeded(String miningChainId, Supplier<UpdateResult> updateOne) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        UpdateResult result = writeChainWithConsumerMigration(miningChainId, updateOne);
        if (result.getMatchedCount() == 0) {
            throw unseededChain(miningChainId);
        }
    }

    /**
     * Writes one split consumer document after moving any embedded predecessors out of the chain record: where
     * it stands, in one write of its own, when it is already there, and otherwise on the fenced path, which may
     * create it. The identity fields are insert-only so a partial update can create the cursor without replacing
     * a different facet written concurrently by the same pipeline.
     */
    private void updateConsumer(String miningChainId, String pipelineId, Document update) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        migrateLegacyConsumers(miningChainId, true);
        Document key = consumerKey(miningChainId, pipelineId);
        if (updateExistingConsumer(miningChainId, key, update)) {
            return;
        }
        update.append("$setOnInsert", consumerIdentity(miningChainId, pipelineId));
        writeConsumer(miningChainId, session -> consumers.updateOne(session,
                key, update, new UpdateOptions().upsert(true)));
    }

    /**
     * Applies {@code update} to the consumer document {@code filter} finds, in one write that holds nothing once
     * it has returned, and answers whether it found one. It never creates one, and that is why it needs no fence
     * against a drop of the chain: a drop before it leaves it nothing to find, a drop after it takes what it
     * wrote, and a drop under way when it arrives is waited for and leaves it nothing either - so nothing it does
     * can bring back a cursor a drop took away.
     */
    private boolean updateExistingConsumer(String miningChainId, Document filter, Document update) {
        return StoreIo.call(miningChainId, () -> consumers.updateOne(filter, update)).getMatchedCount() > 0;
    }

    @FunctionalInterface
    private interface SessionConsumerDocumentMutation {
        boolean apply(ClientSession session, Document consumer);
    }

    /**
     * Checks the root and writes one split cursor - one the write may create - as a single lifecycle operation.
     * The revision increment deliberately writes the root rather than merely reading it: a concurrent
     * {@link #dropChain(String)} then conflicts on that document, so MongoDB serializes the two transactions.
     * If the drop wins, a retry finds no root and refuses the mutation; if this write wins, the later drop
     * removes its cursor.
     */
    private void writeConsumer(String miningChainId, ConsumerWrite write) {
        StoreIo.run(miningChainId, () -> {
            try (ClientSession session = client.startSession()) {
                session.withTransaction(() -> {
                    UpdateResult fenced = collection.updateOne(session,
                            new Document("_id", miningChainId),
                            new Document("$inc", new Document(CONSUMER_WRITE_REVISION, 1L)));
                    if (fenced.getMatchedCount() == 0) {
                        throw unseededChain(miningChainId);
                    }
                    write.apply(session);
                    return null;
                });
            }
        });
    }

    @FunctionalInterface
    private interface ConsumerWrite {
        void apply(ClientSession session);
    }

    /**
     * Makes {@code write} only while {@code fence} is the live claim and the pipeline's cursor on the chain is
     * bound to {@code fence}'s run, in one transaction, and answers what the write answered - empty where the
     * fence did not hold, and where the write itself answered nothing.
     *
     * <p>Proving the claim is a write to it, so a takeover that commits first leaves this nothing to prove, and
     * one that commits after it waits for this to finish: an acknowledgement already in flight when ownership
     * changes lands before the change or not at all. It holds nothing of the chain's own, only this pipeline's
     * claim and cursor, so a member killed halfway through one leaves nothing another pipeline waits on, and
     * what it left open is ended with the rest of what that member left.
     *
     * <p>A binding that is not a document is a damaged record: reported, and never replaced by an
     * acknowledgement.
     */
    private <T> Optional<T> fencedConsumerWrite(String miningChainId, String pipelineId, WorkloadClaimFence fence,
            java.util.function.Function<ClientSession, T> write) {
        Document bound = sinkAckFenceDocument(pipelineId, fence);
        Document key = consumerKey(miningChainId, pipelineId);
        return StoreIo.call(miningChainId, () -> {
            try (ClientSession session = client.startSession()) {
                return session.withTransaction(() -> {
                    if (!provesClaim(session, fence)) {
                        return Optional.<T>empty();
                    }
                    Document cursor = consumers.find(session, key)
                            .projection(Projections.include(SINK_ACK_FENCE)).first();
                    if (cursor == null || !boundTo(cursor, bound)) {
                        return Optional.<T>empty();
                    }
                    if (!bound.equals(cursor.get(SINK_ACK_FENCE))) {
                        // The same run, proved under the topology its holder has taken the claim again under
                        // since the run was bound: the binding records the claim as it now stands.
                        consumers.updateOne(session, key, new Document("$set", new Document(SINK_ACK_FENCE, bound)));
                    }
                    return Optional.ofNullable(write.apply(session));
                });
            }
        });
    }

    /** Whether {@code fence} is the live claim, by a write to it, inside {@code session}'s transaction. */
    private boolean provesClaim(ClientSession session, WorkloadClaimFence fence) {
        return workloadClaims.updateOne(session, WorkloadClaimDocuments.live(fence), PROVE_SINK_CLAIM)
                .getMatchedCount() == 1;
    }

    /**
     * Whether {@code cursor} is bound to the run {@code bound} names. The topology the claim was last taken under
     * is not part of the run: a member joining has the holder take the same claim again, with the same
     * generations, under the new revision, and the run it carries goes on.
     */
    private static boolean boundTo(Document cursor, Document bound) {
        Object stored = cursor.get(SINK_ACK_FENCE);
        if (stored == null) {
            return false;
        }
        if (!(stored instanceof Document binding)) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(cursor.get("_id")), "field", SINK_ACK_FENCE), null);
        }
        Document storedRun = new Document(binding);
        Document boundRun = new Document(bound);
        storedRun.remove(TOPOLOGY_REVISION);
        boundRun.remove(TOPOLOGY_REVISION);
        return boundRun.equals(storedRun);
    }

    /** The field of a stored claim the run's identity leaves out. */
    private static final String TOPOLOGY_REVISION = "topologyRevision";

    /** The persisted run identity beside sink progress; kept exact as a document-shape contract. */
    static Document sinkAckFenceDocument(String pipelineId, WorkloadClaimFence fence) {
        requirePipelineFence(pipelineId, fence);
        return WorkloadClaimDocuments.stored(fence);
    }

    /** A sink fence is a submitted execution of this exact pipeline, never another workload type. */
    private static void requirePipelineFence(String consumerId, WorkloadClaimFence fence) {
        Objects.requireNonNull(consumerId, "consumerId");
        Objects.requireNonNull(fence, "fence");
        if (fence.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || !SrsConsumerId.pipelineOf(consumerId).equals(fence.key().resourceId())
                || fence.executionGeneration() < 1) {
            throw new IllegalArgumentException(
                    "a sink acknowledgement fence must name this pipeline's submitted execution");
        }
    }

    /**
     * Retries a chain-document write after losslessly splitting legacy consumer cursors out of a record
     * that they have already filled. A size failure with no embedded cursors is unchanged: migration has
     * no truthful bytes to reclaim from that document.
     */
    private <T> T writeChainWithConsumerMigration(String miningChainId, Supplier<T> write) {
        try {
            return StoreIo.call(miningChainId, write);
        } catch (TapstateException e) {
            if (e.code() != IoError.DOCUMENT_TOO_LARGE) {
                throw e;
            }
            // Retry even when this call finds the map already empty: another writer may have completed
            // the migration after this write was refused, and rethrowing the stale refusal would turn that
            // successful concurrent repair into a false failure.
            migrateLegacyConsumers(miningChainId, true);
            return StoreIo.call(miningChainId, write);
        }
    }

    /**
     * Copies every cursor from the legacy embedded map into its own document, then clears that map. The
     * copy uses {@code $setOnInsert}: if a previous attempt landed a cursor and stopped before the clear,
     * retrying cannot replace that cursor with the older embedded value. The empty map remains on the root
     * because it is a structural field older readers and the stored-record decoder require.
     */
    private void migrateLegacyConsumers(String miningChainId, boolean requireSeeded) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Document root = StoreIo.call(() -> collection.find(new Document("_id", miningChainId))
                .projection(Projections.include("consumerOffsets"))
                .first());
        Document embedded = embeddedConsumers(root, miningChainId, requireSeeded);
        if (embedded == null || embedded.isEmpty()) {
            return;
        }
        StoreIo.run(miningChainId, () -> {
            try (ClientSession session = client.startSession()) {
                session.withTransaction(() -> {
                    migrateLegacyConsumers(session, miningChainId, requireSeeded);
                    return null;
                });
            }
        });
    }

    /** The transactional body of legacy migration, kept separate because transaction callbacks may retry. */
    private void migrateLegacyConsumers(
            ClientSession session, String miningChainId, boolean requireSeeded) {
        Document root = collection.find(session, new Document("_id", miningChainId))
                .projection(Projections.include("consumerOffsets"))
                .first();
        Document embedded = embeddedConsumers(root, miningChainId, requireSeeded);
        if (embedded == null || embedded.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Object> entry : embedded.entrySet()) {
            String pipelineId = entry.getKey();
            Document identityAndCursor = consumerIdentity(miningChainId, pipelineId);
            identityAndCursor.putAll(asDocument(entry.getValue(), miningChainId));
            insertLegacyConsumer(session, miningChainId, pipelineId, identityAndCursor);
        }
        UpdateResult cleared = collection.updateOne(session,
                new Document("_id", miningChainId),
                new Document("$set", new Document("consumerOffsets", new Document())));
        if (cleared.getMatchedCount() == 0 && requireSeeded) {
            throw unseededChain(miningChainId);
        }
    }

    /** Reads and validates the legacy cursor map, or answers absent for an allowed missing chain. */
    private static Document embeddedConsumers(
            Document root, String miningChainId, boolean requireSeeded) {
        if (root == null) {
            if (requireSeeded) {
                throw unseededChain(miningChainId);
            }
            return null;
        }
        Object raw = root.get("consumerOffsets");
        if (raw instanceof Document embedded) {
            return embedded;
        }
        throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", miningChainId, "field", "consumerOffsets"), null);
    }

    /**
     * Lands one legacy cursor unless an earlier attempt landed it first. This runs in the same transaction
     * as the root clear, so a competing migration either precedes this one or makes its callback retry.
     */
    private void insertLegacyConsumer(ClientSession session, String miningChainId,
            String pipelineId, Document identityAndCursor) {
        consumers.updateOne(session,
                consumerKey(miningChainId, pipelineId),
                new Document("$setOnInsert", identityAndCursor),
                new UpdateOptions().upsert(true));
    }

    /** Reads legacy and split cursors, with the split document winning during an interrupted migration. */
    private List<ConsumerOffset> mergedConsumers(Document root) {
        String miningChainId = root.getString("_id");
        Object raw = root.get("consumerOffsets");
        if (miningChainId == null || !(raw instanceof Document embedded)) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(miningChainId),
                            "field", miningChainId == null ? "_id" : "consumerOffsets"), null);
        }
        Map<String, ConsumerOffset> merged = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : embedded.entrySet()) {
            merged.put(entry.getKey(),
                    consumerFromDocument(entry.getKey(), asDocument(entry.getValue(), miningChainId)));
        }
        List<Document> split = StoreIo.call(() -> consumers.find(consumersOfChain(miningChainId))
                .into(new ArrayList<>()));
        for (Document document : split) {
            String pipelineId = document.getString("pipelineId");
            if (pipelineId == null) {
                throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                        Map.of("id", String.valueOf(document.get("_id")), "field", "pipelineId"), null);
            }
            merged.put(pipelineId, consumerFromDocument(pipelineId, document));
        }
        return List.copyOf(merged.values());
    }

    /** The collision-free id of one cursor document; chain roots keep scalar string ids. */
    private static Document consumerKey(String miningChainId, String pipelineId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        return new Document("_id", new Document("chain", miningChainId).append("pipeline", pipelineId));
    }

    /** Fields every split cursor carries so both lookup directions can use declared indexes. */
    private static Document consumerIdentity(String miningChainId, String pipelineId) {
        Document identity = new Document("miningChainId", miningChainId).append(PIPELINE_ID, pipelineId);
        SrsConsumerId.sourceOf(pipelineId).ifPresent(source -> identity
                .append("ownerPipelineId", SrsConsumerId.pipelineOf(pipelineId)).append("sourceNodeId", source));
        return identity;
    }

    /** One full split cursor document, used by replacement writes. */
    private static Document consumerDocument(String miningChainId, ConsumerOffset offset) {
        Document document = consumerKey(miningChainId, offset.pipelineId());
        document.putAll(consumerIdentity(miningChainId, offset.pipelineId()));
        document.putAll(consumerToDocument(offset));
        return document;
    }

    /** All split cursor documents belonging to one chain. */
    private static Document consumersOfChain(String miningChainId) {
        return new Document("miningChainId", miningChainId);
    }

    /**
     * The caller ordering error every mutator raises on a chain {@code create} has not seeded. One factory
     * because three paths raise it — the pre-read, the epoch increment and the matched count — and the text
     * is read back as the contract's own in the port suite, so a copy of it that drifted would surface in
     * another module, if anywhere.
     */
    private static IllegalStateException unseededChain(String miningChainId) {
        return new IllegalStateException("srs meta mutate on an unseeded mining chain: " + miningChainId
                + " (create must seed it first)");
    }

    /**
     * Classifies a failed seed insert: a duplicate {@code _id} is a caller ordering error (the chain was
     * already seeded), surfaced bare like the unseeded-mutate ordering error — not laundered into an io
     * code that would hide it; any other driver failure is a coded io diagnostic.
     */
    static RuntimeException classifyInsertFailure(MongoException e, String miningChainId) {
        if (e instanceof MongoWriteException write && write.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
            return new IllegalStateException(
                    "create on an already-seeded mining chain: " + miningChainId + " (create is insert-only)", e);
        }
        return StoreIo.coded(e);
    }

    /**
     * Maps the legacy single-document shape used by compatibility reads and their witnesses. New roots are
     * created through this mapping with no consumers; consumer writes persist split documents instead.
     */
    static Document toDocument(SrsMeta meta) {
        Document consumers = new Document();
        for (ConsumerOffset offset : meta.consumerOffsets()) {
            consumers.append(offset.pipelineId(), consumerToDocument(offset));
        }
        List<Document> schemaHistory = new ArrayList<>();
        for (SchemaVersion version : meta.schemaHistory()) {
            schemaHistory.add(schemaToDocument(version));
        }
        // The structural fields are always present (empty when seeded); the nullable positions are
        // appended only when set, so a seed reads back as a seed rather than as corruption.
        Document document = new Document("_id", meta.miningChainId())
                .append("consumerOffsets", consumers)
                .append("schemaHistory", schemaHistory)
                .append(CONSUMER_WRITE_REVISION, 0L);
        if (meta.sourceRead() != null) {
            document.putAll(sourceReadFields(meta.sourceRead(), meta.sourceReadAt()));
        }
        if (meta.sourceReadDurable()) {
            document.append("sourceReadDurable", true);
        }
        if (meta.retention() != null) {
            document.append("retention", meta.retention());
        }
        // Zero means "no generation opened", which is also what an absent field reads back as, so a seed
        // stays a seed rather than carrying a field that says nothing.
        if (meta.epoch() != 0L) {
            document.append("epoch", meta.epoch());
        }
        return document;
    }

    /**
     * Reads one generation counter out of a stored document. Absent is zero rather than corruption: the
     * meta field set is append-only and these fields are newer than the collection, so a document an older
     * build wrote has no generation opened. A stored value of another type is corruption, surfaced as a
     * coded io diagnostic rather than a bare cast failure.
     */
    private static long readEpoch(Document document, String field) {
        Object raw = document.get(field);
        if (raw == null) {
            return 0L;
        }
        if (raw instanceof Number number) {
            return number.longValue();
        }
        throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", String.valueOf(document.get("_id")), "field", field), null);
    }

    /** Reconstructs a meta record from its stored document. */
    static SrsMeta toMeta(Document document) {
        String id = document.getString("_id");
        Object consumersRaw = document.get("consumerOffsets");
        Object schemaRaw = document.get("schemaHistory");
        if (id == null || !(consumersRaw instanceof Document consumersDoc) || !(schemaRaw instanceof List<?> entries)) {
            // A stored meta missing a field this version requires is store corruption, surfaced as a
            // coded io diagnostic rather than a bare cast / unboxing crash while reconstructing.
            // Three grounds, so the name has to be chosen from all three: reporting the second one's
            // field while the first fired sends the reader to a field that is intact, and leaves the
            // one actually missing named nowhere.
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(id),
                            "field", id == null ? "_id"
                                    : consumersRaw instanceof Document ? "schemaHistory" : "consumerOffsets"), null);
        }
        List<ConsumerOffset> consumers = new ArrayList<>();
        for (Map.Entry<String, Object> entry : consumersDoc.entrySet()) {
            consumers.add(consumerFromDocument(entry.getKey(), asDocument(entry.getValue(), id)));
        }
        List<SchemaVersion> schemaHistory = new ArrayList<>();
        for (Object entry : entries) {
            schemaHistory.add(schemaFromDocument(asDocument(entry, id), id));
        }
        return new SrsMeta(id, sourceReadFrom(document), consumers,
                schemaHistory, document.getString("retention"), readEpoch(document, "epoch"),
                sourceReadAtFrom(document), sourceReadDurableFrom(document, id));
    }

    private static boolean sourceReadDurableFrom(Document document, String id) {
        Object raw = document.get("sourceReadDurable");
        if (raw == null) {
            return false;
        }
        if (!(raw instanceof Boolean durable)) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", id, "field", "sourceReadDurable"), null);
        }
        return durable;
    }

    /** Reconstructs one consumer cursor from its stored sub-document, keyed by the pipeline id. */
    private static ConsumerOffset consumerFromDocument(String pipelineId, Document document) {
        ringDoneFromDocument(document, pipelineId);
        Map<String, Long> perTableSeq = new LinkedHashMap<>();
        Object perTableRaw = document.get("perTableSeq");
        if (perTableRaw instanceof Document perTableDoc) {
            for (Map.Entry<String, Object> entry : perTableDoc.entrySet()) {
                perTableSeq.put(entry.getKey(), ((Number) entry.getValue()).longValue());
            }
        } else if (perTableRaw != null) {
            // Present but not a sub-document is store corruption. Absent is a valid sink-ack-only consumer:
            // the sink created the entry (a sinkAckedSrcpos-only $set) before the reader published any
            // per-table cursor, mirroring how an absent sinkAckedSrcpos reads back as null. It reads as an
            // empty cursor rather than as corruption.
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", pipelineId, "field", "perTable"), null);
        }
        return new ConsumerOffset(
                pipelineId,
                perTableSeq,
                sinkAckedFrom(document),
                snapshotCompletedFrom(document),
                document.getString("cdcStartPosition"),
                readEpoch(document, "snapshotEpoch"), confirmedByTable(document, pipelineId),
                progressKind(document, pipelineId));
    }

    private static Map<String, ChainPosition> confirmedByTable(Document document, String consumerId) {
        Document tables = nestedDocument(document, SINK_ACKED_BY_TABLE, consumerId);
        Map<String, ChainPosition> positions = new LinkedHashMap<>();
        if (tables != null) {
            for (Map.Entry<String, Object> entry : tables.entrySet()) {
                if (!(entry.getValue() instanceof Document stored)) {
                    throw unreadableConsumer(consumerId, SINK_ACKED_BY_TABLE);
                }
                ChainPosition position = sinkAckedFrom(stored);
                if (position != null) {
                    positions.put(entry.getKey(), position);
                }
            }
        }
        return positions;
    }

    private static ConsumerProgressKind progressKind(Document document, String consumerId) {
        Object raw = document.get(PROGRESS_KIND);
        if (raw == null) {
            return ConsumerProgressKind.LEGACY;
        }
        if (!(raw instanceof String kind)) {
            throw unreadableConsumer(consumerId, PROGRESS_KIND);
        }
        return switch (kind) {
            case "LEGACY" -> ConsumerProgressKind.LEGACY;
            case "SRS" -> ConsumerProgressKind.SRS;
            case "DIRECT_SOURCE" -> ConsumerProgressKind.DIRECT_SOURCE;
            default -> throw unreadableConsumer(consumerId, PROGRESS_KIND);
        };
    }

    /**
     * The tables one consumer has finished loading, empty when it has finished none. Absent is not
     * corruption: a consumer entry is created by whichever of its three writers gets there first, and the
     * two position writers create it without this field.
     */
    private static List<String> snapshotCompletedFrom(Document document) {
        Object raw = document.get("snapshotCompletedTables");
        List<String> completed = new ArrayList<>();
        if (raw instanceof List<?> entries) {
            for (Object entry : entries) {
                completed.add(String.valueOf(entry));
            }
        }
        return completed;
    }

    /** Reconstructs one schema version from its stored sub-document. */
    private static SchemaVersion schemaFromDocument(Document document, String miningChainId) {
        Long version = document.getLong("version");
        Long ddlSeq = document.getLong("ddlSeq");
        Object schemaRaw = document.get("schema");
        if (version == null || ddlSeq == null || !(schemaRaw instanceof Document schemaDoc)) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", miningChainId, "field", "schemaHistory"), null);
        }
        return new SchemaVersion(version, new LinkedHashMap<>(schemaDoc), ddlSeq);
    }

    /** Reads a nested value as a document, or surfaces store corruption when it is not one. */
    private static Document asDocument(Object value, String miningChainId) {
        if (value instanceof Document document) {
            return document;
        }
        throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", miningChainId, "field", "schema"), null);
    }

    /**
     * The acked position a consumer document carries, or null when it has none. A record whose token was
     * written without the order it sat at cannot be ranked against the reader's position, and reads back as
     * nothing acked: that pins a source-read advance where it stands, which only ever costs re-mining
     * changes already read - the direction that keeps them re-minable at all.
     */
    /**
     * How far the chain has read, or null when nothing has read it — the token, with the order beside it
     * when one was recorded.
     *
     * <p>A token with no order is a real state and reads back as the position it is, not as absence.
     * Two things produce it: a write-back, which names a spot in the source's log that no ring ever
     * assigned a coordinate to, and a document written before the order was recorded at all. Reading
     * either as nothing read would drop the one thing both of them do say — where to resume — and send
     * the tail to the snapshot seam instead, re-mining every change since.
     */
    private static ChainPosition sourceReadFrom(Document document) {
        String token = document.getString("sourceReadOffset");
        Object epoch = document.get("sourceReadEpoch");
        Object seq = document.get("sourceReadSeq");
        if (!(epoch instanceof Number) || !(seq instanceof Number)) {
            return token == null ? null : new ChainPosition(null, token);
        }
        return new ChainPosition(
                new SourceOrder(((Number) epoch).longValue(), ((Number) seq).longValue()), token);
    }

    /** When the read offset was last written, or null on a record whose offset predates the stamp. */
    private static Instant sourceReadAtFrom(Document document) {
        Object at = document.get("sourceReadAt");
        return at instanceof Number millis ? Instant.ofEpochMilli(millis.longValue()) : null;
    }

    private static ChainPosition sinkAckedFrom(Document document) {
        Object epoch = document.get("sinkAckedEpoch");
        Object seq = document.get("sinkAckedSeq");
        if (!(epoch instanceof Number) || !(seq instanceof Number)) {
            return null;
        }
        return new ChainPosition(
                new SourceOrder(((Number) epoch).longValue(), ((Number) seq).longValue()),
                document.getString("sinkAckedSrcpos"));
    }

    /**
     * Maps one consumer's record to its stored sub-document: the per-table read cursor, the tables it has
     * finished loading (omitted while it has finished none, so a cursor-only consumer stays a cursor-only
     * consumer), the snapshot start pair and the acked position.
     */
    private static Document consumerToDocument(ConsumerOffset offset) {
        Document perTable = new Document();
        for (Map.Entry<String, Long> entry : offset.perTableSeq().entrySet()) {
            perTable.append(entry.getKey(), entry.getValue());
        }
        Document document = new Document("perTableSeq", perTable);
        if (offset.progressKind() != ConsumerProgressKind.LEGACY) {
            document.append(PROGRESS_KIND, offset.progressKind().name());
        }
        if (!offset.sinkAckedByTable().isEmpty()) {
            Document confirmed = new Document();
            offset.sinkAckedByTable().forEach((table, position) -> {
                Document stored = new Document();
                writePosition(stored, position);
                confirmed.put(table, stored);
            });
            document.append(SINK_ACKED_BY_TABLE, confirmed);
        }
        if (!offset.snapshotCompletedTables().isEmpty()) {
            document.append("snapshotCompletedTables", List.copyOf(offset.snapshotCompletedTables()));
        }
        if (offset.cdcStartPosition() != null) {
            document.append("cdcStartPosition", offset.cdcStartPosition());
        }
        if (offset.snapshotEpoch() != 0L) {
            document.append("snapshotEpoch", offset.snapshotEpoch());
        }
        if (offset.sinkAcked() != null) {
            document.append("sinkAckedEpoch", offset.sinkAcked().order().epoch())
                    .append("sinkAckedSeq", offset.sinkAcked().order().seq());
            if (offset.sinkAcked().token() != null) {
                document.append("sinkAckedSrcpos", offset.sinkAcked().token());
            }
        }
        return document;
    }

    /** Maps one schema version to its stored sub-document. */
    private static Document schemaToDocument(SchemaVersion version) {
        return new Document("version", version.version())
                .append("schema", new Document(version.schema()))
                .append("ddlSeq", version.ddlSeq());
    }
}
