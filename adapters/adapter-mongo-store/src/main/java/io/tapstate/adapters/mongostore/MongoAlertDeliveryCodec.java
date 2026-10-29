package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertIntent.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.bson.Document;
import org.bson.types.Binary;

/** Closed intent encoding. No class names, reflective enum restoration or arbitrary payload objects. */
final class MongoAlertDeliveryCodec {
    private static final int METADATA_RESERVATION = 4096;
    private static final String MAX_RECEIPT_TEXT = new String(Character.toChars(0x1f680)).repeat(256);
    private static final Set<String> FIELDS = Set.of("_id", "formatVersion", "recordRevision", "channelType", "channelVersion",
            "deadline", "quotaMinute", "cleanupAt", "nextCheckAt", "frozen", "state");
    record Stored(long recordRevision, AlertIntent.Record value) { }
    private MongoAlertDeliveryCodec() { }

    static Document key(Identity identity) {
        return scope(identity.scope()).append("purpose", identity instanceof Business ? "business" : "channel-test")
                .append("deliveryId", identity.deliveryId());
    }
    static Document write(AlertIntent.Record value, long revision) {
        if (revision < 1) { throw new IllegalArgumentException("stored revision must be positive"); }
        Frozen frozen = value.frozen(); State state = value.state();
        var encoded = new Document("_id", key(frozen.identity())).append("formatVersion", frozen.testAuthority() == null ? AlertIntent.FORMAT_VERSION : 2)
                .append("recordRevision", revision).append("channelType", frozen.channel().typeId()).append("channelVersion", frozen.channel().version())
                .append("deadline", time(frozen.deadline())).append("quotaMinute", frozen.identity() instanceof Test test
                        ? time(AlertTestQuota.minute(test.command().acceptedAt())) : null)
                .append("cleanupAt", MongoAlertQueryProjection.intentCleanup(value)).append("nextCheckAt", MongoAlertQueryProjection.intentCheck(value))
                .append("frozen", frozen(frozen)).append("state", state(state));
        budget(encoded, frozen.retry().maximumTotalAttempts()); return encoded;
    }

    static Stored read(Identity expected, Document row) {
        try {
            fields(row, FIELDS); require(MongoAlertConfigurationCodec.size(row) <= AlertIntent.MAX_BYTES);
            sameKey(key(expected), doc(row, "_id")); require(Integer.valueOf(1).equals(row.get("formatVersion")) || Integer.valueOf(2).equals(row.get("formatVersion")));
            long revision = number(row, "recordRevision", 1);
            Frozen frozen = readFrozen(doc(row, "frozen")); require(sameIdentity(frozen.identity(), expected));
            require(Integer.valueOf(frozen.testAuthority() == null ? 1 : 2).equals(row.get("formatVersion")));
            require(frozen.channel().typeId().equals(text(row, "channelType")) && frozen.channel().version() == integer(row, "channelVersion", 1));
            require(frozen.deadline().equals(instant(row, "deadline", false)));
            require(Objects.equals(row.get("quotaMinute"), expected instanceof Test test ? time(AlertTestQuota.minute(test.command().acceptedAt())) : null));
            var value = new AlertIntent.Record(frozen, readState(frozen, doc(row, "state")));
            require(MongoAlertQueryProjection.intentCleanup(value).equals(row.get("cleanupAt"))
                    && Objects.equals(MongoAlertQueryProjection.intentCheck(value), row.get("nextCheckAt")));
            budget(row, frozen.retry().maximumTotalAttempts()); return new Stored(revision, value);
        } catch (IllegalArgumentException | NullPointerException | ArithmeticException | java.time.DateTimeException | TapstateException invalid) {
            throw unreadable("format-or-identity");
        }
    }

    static Stored readAny(AlarmKey.Scope scope,Document row) {
        Frozen frozen=readFrozen(doc(row,"frozen")); require(frozen.identity().scope().equals(scope));return read(frozen.identity(),row);
    }

    static Stored readTest(AlertTestCommand.Key expected, Document row) {
        try {
            require(MongoAlertConfigurationCodec.size(row) <= AlertIntent.MAX_BYTES);
            Frozen frozen = readFrozen(doc(row, "frozen"));
            require(frozen.identity() instanceof Test test && test.command().key().equals(expected));
            return read(frozen.identity(), row);
        } catch (IllegalArgumentException | NullPointerException | ArithmeticException | java.time.DateTimeException | TapstateException invalid) {
            throw unreadable("test-command-format-or-identity");
        }
    }

    private static boolean sameIdentity(Identity left, Identity right) {
        if (left instanceof Business a && right instanceof Business b) { return a.key().equals(b.key()); }
        return left instanceof Test a && right instanceof Test b && a.command().key().equals(b.command().key());
    }

    /** Inspection preserves missing/unsupported capabilities; this header never authorizes execution. */
    static DeliveryCapabilityPolicy.Header header(Document row) {
        Integer version = row.get("formatVersion") instanceof Integer value ? value : null;
        Integer channelVersion = row.get("channelVersion") instanceof Integer value ? value : null;
        AlarmKey.Scope scope = null; String deliveryId = null;
        if (row.get("_id") instanceof Document id) {
            try { scope = readScope(id); } catch (IllegalArgumentException | TapstateException invalid) { /* Keep the missing identity visible. */ }
            if (id.get("deliveryId") instanceof String value && value.matches("[A-Za-z0-9._-]{1,256}")) { deliveryId = value; }
        }
        String channel = row.get("channelType") instanceof String value && value.matches("[a-z][a-z0-9]*([.-][a-z0-9]+)*")
                && value.length() <= 64 ? value : null;
        Instant deadline = null;
        try { deadline = instant(row, "deadline", false); } catch (IllegalArgumentException | java.time.DateTimeException invalid) { /* No invented cutoff. */ }
        return new DeliveryCapabilityPolicy.Header(version, scope, deliveryId, channel, channelVersion, deadline);
    }

    private static Document frozen(Frozen value) {
        Document encoded = new Document("identity", identity(value.identity())).append("channel", new Document("type", value.channel().typeId())
                .append("version", value.channel().version()).append("family", value.channel().family().name()))
                .append("route", route(value.route())).append("target", target(value.target())).append("content", content(value.content()))
                .append("members", value.members().stream().map(MongoAlertDeliveryCodec::member).toList())
                .append("retry", retry(value.retry())).append("retention", policy(value.retention()))
                .append("createdAt", time(value.createdAt())).append("deadline", time(value.deadline()));
        if (value.testAuthority() != null) { encoded.append("testAuthority", authority(value.testAuthority())); }
        return encoded;
    }
    private static Frozen readFrozen(Document value) {
        var expected = new java.util.HashSet<>(Set.of("identity", "channel", "route", "target", "content", "members", "retry", "retention", "createdAt", "deadline"));
        if (value.containsKey("testAuthority")) { expected.add("testAuthority"); }
        fields(value, expected);
        Document channel = doc(value, "channel"); fields(channel, Set.of("type", "version", "family"));
        List<Member> members = new ArrayList<>(); for (Object item : list(value, "members", 100)) { members.add(readMember(document(item))); }
        return new Frozen(readIdentity(doc(value, "identity")), new AlertChannelPolicy.Capability(text(channel, "type"), integer(channel, "version", 1),
                enumeration(AlertChannelPolicy.Family.values(), text(channel, "family"))), value.get("route") == null ? null : readRoute(doc(value, "route")),
                readTarget(doc(value, "target")), readContent(doc(value, "content")), members, readRetry(doc(value, "retry")),
                readPolicy(doc(value, "retention")), instant(value, "createdAt", false), instant(value, "deadline", false),
                value.containsKey("testAuthority") ? readAuthority(doc(value,"testAuthority")) : null);
    }
    static Document authority(AlertTestAuthority value) {
        return new Document("mechanism",value.mechanism().name()).append("realm",value.realm()).append("subject",value.subject())
                .append("grantedScope",value.grantedScope()).append("credentialBinding",value.credentialBinding()).append("expiresAt",time(value.expiresAt()));
    }
    static AlertTestAuthority readAuthority(Document row) {
        fields(row,Set.of("mechanism","realm","subject","grantedScope","credentialBinding","expiresAt"));
        return new AlertTestAuthority(enumeration(AlertTestAuthority.Mechanism.values(),text(row,"mechanism")),text(row,"realm"),text(row,"subject"),
                text(row,"grantedScope"),text(row,"credentialBinding"),instant(row,"expiresAt",true));
    }
    static Document deliveryKey(DeliveryKey value) {
        var n = value.notification();
        return new Document("notification", scope(n.scope()).append("subjectId", n.subjectId()).append("subjectIncarnation", n.subjectIncarnation())
                .append("routeId", n.routeId()).append("routeIncarnation", n.routeIncarnation()).append("kind", n.kind().name()).append("round", n.round()))
                .append("channelType", value.channelType()).append("receiverIncarnation", value.receiverIncarnation()).append("targetId", value.targetId());
    }
    static DeliveryKey readDeliveryKey(Document value) {
        fields(value, Set.of("notification", "channelType", "receiverIncarnation", "targetId"));
        Document n = doc(value, "notification"); fields(n, Set.of("authorityId", "clusterId", "subjectId", "subjectIncarnation", "routeId", "routeIncarnation", "kind", "round"));
        return new DeliveryKey(new DeliveryKey.NotificationRound(readScope(n), text(n, "subjectId"), text(n, "subjectIncarnation"), text(n, "routeId"),
                text(n, "routeIncarnation"), enumeration(DeliveryKey.Kind.values(), text(n, "kind")), number(n, "round", 1)),
                text(value, "channelType"), text(value, "receiverIncarnation"), text(value, "targetId"));
    }
    private static Document identity(Identity value) {
        return value instanceof Business business ? new Document("purpose", "business").append("key", deliveryKey(business.key()))
                : new Document("purpose", "channel-test").append("command", command(((Test) value).command()));
    }
    static Identity readIdentity(Document value) {
        return switch (text(value, "purpose")) {
            case "business" -> { fields(value, Set.of("purpose", "key")); yield new Business(readDeliveryKey(doc(value, "key"))); }
            case "channel-test" -> { fields(value, Set.of("purpose", "command")); yield new Test(readCommand(doc(value, "command"))); }
            default -> throw new IllegalArgumentException("unknown intent purpose");
        };
    }
    static Document command(AlertTestCommand.Accepted value) {
        var key = value.key(); var p = value.parameters();
        return new Document("key", scope(key.scope()).append("actor", key.actorPrincipal()).append("clientId", key.clientId().value()))
                .append("parameters", new Document("channelType", p.channelType()).append("candidateId", p.candidate().id()).append("candidateDigest", p.candidate().digest())
                        .append("receiver", p.receiver() == null ? null : new Document("incarnation", p.receiver().incarnation()).append("expectedRevision", p.receiver().expectedRevision()))
                        .append("targetId", p.targetId()).append("bindingId", p.bindingId()).append("route", p.route() == null ? null
                                : new Document("incarnation", p.route().incarnation()).append("revision", p.route().revision()).append("settingsRevision", p.route().messageSettingsRevision()))
                        .append("payloadSha256", p.payloadSha256())).append("acceptedAt", time(value.acceptedAt())).append("deadline", time(value.deadline()));
    }
    static AlertTestCommand.Accepted readCommand(Document value) {
        fields(value, Set.of("key", "parameters", "acceptedAt", "deadline"));
        Document key = doc(value, "key"); fields(key, Set.of("authorityId", "clusterId", "actor", "clientId"));
        Document p = doc(value, "parameters"); fields(p, Set.of("channelType", "candidateId", "candidateDigest", "receiver", "targetId", "bindingId", "route", "payloadSha256"));
        AlertTestCommand.ExistingReceiver receiver = null; AlertTestCommand.Route route = null;
        if (p.get("receiver") != null) { Document r = doc(p, "receiver"); fields(r, Set.of("incarnation", "expectedRevision")); receiver = new AlertTestCommand.ExistingReceiver(text(r, "incarnation"), number(r, "expectedRevision", 1)); }
        if (p.get("route") != null) { Document r = doc(p, "route"); fields(r, Set.of("incarnation", "revision", "settingsRevision")); route = new AlertTestCommand.Route(text(r, "incarnation"), number(r, "revision", 1), number(r, "settingsRevision", 1)); }
        return new AlertTestCommand.Accepted(new AlertTestCommand.Key(readScope(key), text(key, "actor"), AlertTestCommand.ClientId.parse(text(key, "clientId"))),
                new AlertTestCommand.Parameters(text(p, "channelType"), new AlertTestCommand.Candidate(text(p, "candidateId"), text(p, "candidateDigest")),
                        receiver, text(p, "targetId"), text(p, "bindingId"), route, text(p, "payloadSha256")),
                instant(value, "acceptedAt", false), instant(value, "deadline", false));
    }
    static Document route(Route value) {
        if (value == null) { return null; }
        return value instanceof ConfiguredRoute r ? new Document("kind", "configured").append("identity", MongoAlertConfigurationCodec.identity(r.identity())).append("bindingRevision", r.bindingRevision())
                : new Document("kind", "system").append("id", value.id()).append("incarnation", value.incarnation()).append("profileVersion", ((SystemRoute) value).profileVersion());
    }
    static Route readRoute(Document value) {
        return switch (text(value, "kind")) {
            case "configured" -> { fields(value, Set.of("kind", "identity", "bindingRevision")); yield new ConfiguredRoute(MongoAlertConfigurationCodec.readIdentity(doc(value, "identity")), number(value, "bindingRevision", 1)); }
            case "system" -> { fields(value, Set.of("kind", "id", "incarnation", "profileVersion")); yield new SystemRoute(text(value, "id"), text(value, "incarnation"), integer(value, "profileVersion", 1)); }
            default -> throw new IllegalArgumentException("unknown route snapshot");
        };
    }
    private static Document target(Target value) {
        return switch (value) {
            case ReceiverTarget r -> new Document("kind", "receiver").append("identity", MongoAlertConfigurationCodec.identity(r.identity())).append("bindingRevision", r.bindingRevision())
                    .append("transport", MongoAlertReceiverCodec.transport(r.transport())).append("credentials", MongoAlertReceiverCodec.credentials(r.credentials()));
            case SdkTarget s -> new Document("kind", "sdk").append("incarnation", s.incarnation()).append("logicalTargetId", s.logicalTargetId()).append("clusterBinding", s.clusterBinding());
            case CandidateTarget c -> new Document("kind", "candidate").append("transport", MongoAlertReceiverCodec.transport(c.transport()))
                    .append("existingReceiver", c.existingReceiver() == null ? null : MongoAlertConfigurationCodec.identity(c.existingReceiver()))
                    .append("bindingRevision", c.receiverBindingRevision()).append("materialSha256", c.encryptedMaterialSha256());
        };
    }
    static Target readTarget(Document value) {
        return switch (text(value, "kind")) {
            case "receiver" -> {
                fields(value, Set.of("kind", "identity", "bindingRevision", "transport", "credentials"));
                var id = MongoAlertConfigurationCodec.readIdentity(doc(value, "identity"));
                yield new ReceiverTarget(id, number(value, "bindingRevision", 1), MongoAlertReceiverCodec.readTransport(doc(value, "transport")),
                        value.get("credentials") == null ? null : MongoAlertReceiverCodec.readCredentials(id, doc(value, "credentials")));
            }
            case "sdk" -> { fields(value, Set.of("kind", "incarnation", "logicalTargetId", "clusterBinding")); yield new SdkTarget(text(value, "incarnation"), text(value, "logicalTargetId"), text(value, "clusterBinding")); }
            case "candidate" -> {
                fields(value, Set.of("kind", "transport", "existingReceiver", "bindingRevision", "materialSha256"));
                yield new CandidateTarget(MongoAlertReceiverCodec.readTransport(doc(value, "transport")), value.get("existingReceiver") == null ? null
                        : MongoAlertConfigurationCodec.readIdentity(doc(value, "existingReceiver")), number(value, "bindingRevision", 0), nullableText(value, "materialSha256"));
            }
            default -> throw new IllegalArgumentException("unknown target snapshot");
        };
    }
    private static Document content(Content value) {
        var t = value.template(); var m = t.settings();
        return new Document("contentType", value.contentType()).append("payloadSha256", value.payloadSha256()).append("renderingFailed", value.renderingFailed())
                .append("template", new Document("id", t.id()).append("version", t.version()).append("settingsRevision", t.settingsRevision()).append("language", t.language())
                        .append("settings", new Document("titlePrefix", m.titlePrefix()).append("titleSuffix", m.titleSuffix()).append("messageNote", m.messageNote()).append("footerText", m.footerText())));
    }
    private static Content readContent(Document value) {
        fields(value, Set.of("contentType", "payloadSha256", "renderingFailed", "template"));
        Document t = doc(value, "template"); fields(t, Set.of("id", "version", "settingsRevision", "language", "settings"));
        Document m = doc(t, "settings"); fields(m, Set.of("titlePrefix", "titleSuffix", "messageNote", "footerText"));
        return new Content(text(value, "contentType"), nullableText(value, "payloadSha256"), new Template(text(t, "id"), integer(t, "version", 1), number(t, "settingsRevision", 1),
                text(t, "language"), new MessageSettings(text(m, "titlePrefix"), text(m, "titleSuffix"), text(m, "messageNote"), text(m, "footerText"))), bool(value, "renderingFailed"));
    }
    static Document member(Member value) {
        return new Document("alarm", MongoAlertStateCodec.key(value.evaluation().key())).append("evaluationBinding", MongoAlertStateCodec.binding(value.evaluation()))
                .append("episode", value.episode()).append("ruleIdentity", MongoAlertConfigurationCodec.identity(value.ruleIdentity())).append("sourceBindingRevision", value.sourceBindingRevision());
    }
    static Member readMember(Document value) {
        fields(value, Set.of("alarm", "evaluationBinding", "episode", "ruleIdentity", "sourceBindingRevision"));
        var key = MongoAlertStateCodec.readKey(doc(value, "alarm"));
        return new Member(MongoAlertStateCodec.readBinding(key, doc(value, "evaluationBinding")), number(value, "episode", 0),
                MongoAlertConfigurationCodec.readIdentity(doc(value, "ruleIdentity")), number(value, "sourceBindingRevision", 0));
    }
    private static Document retry(ChannelRetryPolicy value) {
        return new Document("attemptsPerCycle", value.attemptsPerCycle()).append("maximumManualCycles", value.maximumManualCycles())
                .append("delaysNanos", value.retryDelays().stream().map(Duration::toNanos).toList()).append("maximumAgeNanos", value.maximumAge().toNanos()).append("attemptTimeoutNanos", value.attemptTimeout().toNanos());
    }
    private static ChannelRetryPolicy readRetry(Document value) {
        fields(value, Set.of("attemptsPerCycle", "maximumManualCycles", "delaysNanos", "maximumAgeNanos", "attemptTimeoutNanos"));
        List<Duration> delays = new ArrayList<>();
        for (Object delay : list(value, "delaysNanos", 9)) { require(delay instanceof Long n && n >= 0); delays.add(Duration.ofNanos((Long) delay)); }
        return new ChannelRetryPolicy(integer(value, "attemptsPerCycle", 1), integer(value, "maximumManualCycles", 0), delays,
                Duration.ofNanos(number(value, "maximumAgeNanos", 1)), Duration.ofNanos(number(value, "attemptTimeoutNanos", 1)));
    }
    static Document policy(Policy value) {
        Document values = new Document(); value.values().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> values.append(entry.getKey().name(), entry.getValue().toNanos()));
        return new Document("policyVersion", AlertRetentionPolicy.POLICY_VERSION).append("deployment", value.deployment().name()).append("valuesNanos", values);
    }
    static Policy readPolicy(Document value) {
        fields(value, Set.of("policyVersion", "deployment", "valuesNanos")); require(Integer.valueOf(AlertRetentionPolicy.POLICY_VERSION).equals(value.get("policyVersion")));
        var values = new EnumMap<AlertRetentionPolicy.Setting, Duration>(AlertRetentionPolicy.Setting.class);
        doc(value, "valuesNanos").forEach((key, duration) -> { require(duration instanceof Long n && n > 0); values.put(enumeration(AlertRetentionPolicy.Setting.values(), key), Duration.ofNanos((Long) duration)); });
        return new Policy(enumeration(AlertChannelPolicy.Deployment.values(), text(value, "deployment")), values);
    }
    static Document owner(AlertOwnership value) { return new Document("generation", value.generation()).append("ownerId", value.ownerId()).append("leaseUntil", time(value.leaseUntil())); }
    static AlertOwnership readOwner(Document value) {
        fields(value, Set.of("generation", "ownerId", "leaseUntil")); return new AlertOwnership(number(value, "generation", 0), nullableText(value, "ownerId"), instant(value, "leaseUntil", true));
    }
    private static Document state(State value) {
        var r = value.retained();
        return new Document("owner", owner(value.owner())).append("manualCycles", value.manualCycles()).append("permitsStopped", value.permitsStopped())
                .append("stopReason", value.stopReason() == null ? null : value.stopReason().name()).append("nextAttemptAt", time(value.nextAttemptAt()))
                .append("attemptsConsumed", value.attemptsConsumed()).append("attempts", value.attempts().stream().map(MongoAlertDeliveryCodec::attempt).toList()).append("summary", value.summary().name())
                .append("retained", new Document("payloadUntil", time(r.payloadUntil())).append("resultUntil", time(r.resultUntil())).append("acceptedAt", time(r.acceptedAt())).append("stoppedAt", time(r.stoppedAt())))
                .append("payload", value.payload() == null ? null : new Binary(value.payload().bytes()))
                .append("candidate", candidate(value.candidate()))
                .append("payloadPrunedAt", time(value.payloadPrunedAt())).append("candidatePrunedAt", time(value.candidatePrunedAt())).append("detailsPrunedAt", time(value.detailsPrunedAt()))
                .append("permitsStoppedAt", time(value.permitsStoppedAt())).append("lastAttemptEnd", time(value.lastAttemptEnd())).append("cycleStarts", value.cycleStarts())
                .append("acceptedReceipt", receipt(value.acceptedReceipt()));
    }
    private static State readState(Frozen frozen, Document value) {
        fields(value, Set.of("owner", "manualCycles", "permitsStopped", "stopReason", "nextAttemptAt", "attemptsConsumed", "attempts", "summary", "retained", "payload", "candidate", "payloadPrunedAt", "candidatePrunedAt", "detailsPrunedAt", "permitsStoppedAt", "lastAttemptEnd", "cycleStarts", "acceptedReceipt"));
        List<Attempt> attempts = new ArrayList<>(); for (Object item : list(value, "attempts", 30)) { attempts.add(readAttempt(frozen, document(item))); }
        Document r = doc(value, "retained"); fields(r, Set.of("payloadUntil", "resultUntil", "acceptedAt", "stoppedAt"));
        EncryptedCandidate candidate = null;
        if (value.get("candidate") != null) {
            Document c = doc(value, "candidate"); fields(c, Set.of("formatVersion", "keyId", "ciphertext")); require(Integer.valueOf(1).equals(c.get("formatVersion")));
            candidate = new EncryptedCandidate(text(c, "keyId"), text(c, "ciphertext"));
            candidate(candidate);
        }
        Payload payload = null;
        if (value.get("payload") != null) { require(value.get("payload") instanceof Binary binary && binary.getType() == 0); payload = new Payload(((Binary) value.get("payload")).getData()); }
        List<Integer> cycles = new ArrayList<>(); for (Object offset : list(value, "cycleStarts", 3)) { require(offset instanceof Integer); cycles.add((Integer) offset); }
        return new State(readOwner(doc(value, "owner")), integer(value, "manualCycles", 0), bool(value, "permitsStopped"), value.get("stopReason") == null ? null
                : enumeration(StopReason.values(), text(value, "stopReason")), instant(value, "nextAttemptAt", true), integer(value, "attemptsConsumed", 0), attempts,
                enumeration(DeliveryEvidence.Outcome.values(), text(value, "summary")), new Retained(instant(r, "payloadUntil", false), instant(r, "resultUntil", false),
                        instant(r, "acceptedAt", true), instant(r, "stoppedAt", true)), payload, candidate,
                instant(value, "payloadPrunedAt", true), instant(value, "candidatePrunedAt", true), instant(value, "detailsPrunedAt", true),
                instant(value, "permitsStoppedAt", true), instant(value, "lastAttemptEnd", true), cycles,
                value.get("acceptedReceipt") == null ? null : readReceipt(doc(value, "acceptedReceipt")));
    }
    private static Document candidate(EncryptedCandidate value) {
        if (value == null) { return null; }
        require(value.ciphertext().matches("tscfg:1:[a-f0-9]{64}:[A-Za-z0-9_-]+")
                && value.keyId().equals(SourceConfigCipher.envelopeKeyId(value.ciphertext())));
        String payload = value.ciphertext().substring(value.ciphertext().lastIndexOf(':') + 1);
        byte[] decoded = java.util.Base64.getUrlDecoder().decode(payload);
        require(decoded.length >= 28 && java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(decoded).equals(payload));
        return new Document("formatVersion", 1).append("keyId", value.keyId()).append("ciphertext", value.ciphertext());
    }
    private static Document attempt(Attempt value) {
        var p = value.permit();
        return new Document("attemptId", p.attemptId()).append("ordinal", p.ordinal()).append("ownerGeneration", p.ownerGeneration()).append("ownerId", p.ownerId())
                .append("grantedAt", time(p.grantedAt())).append("deadline", time(p.deadline())).append("outcome", value.outcome().name()).append("endedAt", time(value.endedAt()))
                .append("acceptedRecordedAt", time(value.acceptedRecordedAt())).append("protocol", protocol(value.protocol()))
                .append("receipt", receipt(value.receipt()));
    }
    private static Attempt readAttempt(Frozen frozen, Document value) {
        fields(value, Set.of("attemptId", "ordinal", "ownerGeneration", "ownerId", "grantedAt", "deadline", "outcome", "endedAt", "acceptedRecordedAt", "protocol", "receipt"));
        require(MongoAlertConfigurationCodec.size(value) <= AlertIntent.MAX_ATTEMPT_BYTES);
        Receipt receipt = null;
        if (value.get("receipt") != null) { receipt = readReceipt(doc(value, "receipt")); }
        return new Attempt(new DeliveryAttempt.Permit(frozen.identity().scope(), frozen.identity().deliveryId(), text(value, "attemptId"), integer(value, "ordinal", 1),
                number(value, "ownerGeneration", 1), text(value, "ownerId"), instant(value, "grantedAt", false), instant(value, "deadline", false)),
                enumeration(DeliveryEvidence.Outcome.values(), text(value, "outcome")), instant(value, "endedAt", true), instant(value, "acceptedRecordedAt", true),
                value.get("protocol") == null ? null : readProtocol(doc(value, "protocol")), receipt);
    }
    private static Document receipt(Receipt value) {
        return value == null ? null : new Document("source", value.source()).append("remoteId", value.remoteId()).append("requestId", value.requestId());
    }
    private static Receipt readReceipt(Document value) {
        fields(value, Set.of("source", "remoteId", "requestId")); return new Receipt(text(value, "source"), text(value, "remoteId"), nullableText(value, "requestId"));
    }
    private static Document protocol(Protocol value) {
        return value == null ? null : new Document("phase", value.phase().name()).append("action", value.action().name()).append("status", value.status())
                .append("implementation", value.implementation()).append("retryAfter", time(value.retryAfter()));
    }
    private static Protocol readProtocol(Document value) {
        fields(value, Set.of("phase", "action", "status", "implementation", "retryAfter")); require(MongoAlertConfigurationCodec.size(value) <= AlertIntent.MAX_DIAGNOSTIC_BYTES);
        return new Protocol(enumeration(Phase.values(), text(value, "phase")), enumeration(Action.values(), text(value, "action")), integer(value, "status", 0),
                text(value, "implementation"), instant(value, "retryAfter", true));
    }

    private static void budget(Document row, int maximumAttempts) {
        Document state = doc(row, "state"); Document metadata = new Document(state); metadata.remove("attempts"); metadata.remove("payload"); metadata.remove("candidate");
        requireBudget(MongoAlertConfigurationCodec.size(metadata) <= METADATA_RESERVATION, "controlBytes");
        for (Object item : list(state, "attempts", 30)) {
            Document attempt = document(item); requireBudget(MongoAlertConfigurationCodec.size(attempt) <= AlertIntent.MAX_ATTEMPT_BYTES, "attemptBytes");
            if (attempt.get("protocol") instanceof Document diagnostic) { requireBudget(MongoAlertConfigurationCodec.size(diagnostic) <= AlertIntent.MAX_DIAGNOSTIC_BYTES, "diagnosticBytes"); }
            // Reserve the largest valid accepted receipt before a permit can become durable.
            Document worst = new Document(attempt).append("outcome", "ACCEPTED").append("endedAt", Instant.MAX.toString()).append("acceptedRecordedAt", Instant.MAX.toString())
                    .append("protocol", new Document("phase", "AUTHENTICATION").append("action", "STOP").append("status", 999)
                            .append("implementation", "a".repeat(64)).append("retryAfter", Instant.MAX.toString()))
                    .append("receipt", new Document("source", "sdk-event-center").append("remoteId", MAX_RECEIPT_TEXT).append("requestId", MAX_RECEIPT_TEXT));
            requireBudget(MongoAlertConfigurationCodec.size(worst) <= AlertIntent.MAX_ATTEMPT_BYTES, "attemptResultReservation");
        }
        Document baseline = new Document(row).append("state", new Document("payload", state.get("payload")).append("candidate", state.get("candidate")));
        long reserved = (long) MongoAlertConfigurationCodec.size(baseline) + METADATA_RESERVATION + (long) maximumAttempts * (AlertIntent.MAX_ATTEMPT_BYTES + 4);
        requireBudget(MongoAlertConfigurationCodec.size(row) <= AlertIntent.MAX_BYTES && reserved <= AlertIntent.MAX_BYTES, "intentBytes");
    }
    private static void requireBudget(boolean condition, String field) {
        if (!condition) { throw new TapstateException(AlertError.INVALID_CONFIGURATION, Map.of("field", field, "reason", "encoded-intent-or-result-reservation-too-large"), null); }
    }
    static Document scope(AlarmKey.Scope value) { return new Document("authorityId", value.authorityId()).append("clusterId", value.clusterId()); }
    static AlarmKey.Scope readScope(Document value) { return new AlarmKey.Scope(text(value, "authorityId"), text(value, "clusterId")); }
    static void sameKey(Document expected, Document actual) { require(expected.equals(actual) && List.copyOf(expected.keySet()).equals(List.copyOf(actual.keySet()))); }
    static String time(Instant value) { return value == null ? null : value.toString(); }
    static Instant instant(Document row, String name, boolean nullable) {
        if (nullable && row.get(name) == null) { return null; }
        String value = text(row, name); require(value.length() <= 40); Instant parsed = Instant.parse(value); require(parsed.toString().equals(value)); return parsed;
    }
    static String text(Document row, String name) { require(row.get(name) instanceof String); return row.getString(name); }
    static String nullableText(Document row, String name) { return row.get(name) == null ? null : text(row, name); }
    static long number(Document row, String name, long minimum) { require(row.get(name) instanceof Long n && n >= minimum); return row.getLong(name); }
    static int integer(Document row, String name, int minimum) { require(row.get(name) instanceof Integer n && n >= minimum); return row.getInteger(name); }
    static boolean bool(Document row, String name) { require(row.get(name) instanceof Boolean); return row.getBoolean(name); }
    static Document document(Object value) { require(value instanceof Document); return (Document) value; }
    static Document doc(Document row, String name) { return document(row.get(name)); }
    static List<?> list(Document row, String name, int maximum) { require(row.get(name) instanceof List<?> list && list.size() <= maximum); return (List<?>) row.get(name); }
    static void fields(Document row, Set<String> expected) { require(row != null && row.keySet().equals(expected)); }
    static void require(boolean value) { if (!value) { throw new IllegalArgumentException("invalid delivery record encoding"); } }
    static <E extends Enum<E>> E enumeration(E[] known, String text) { return Arrays.stream(known).filter(value -> value.name().equals(text)).findFirst().orElseThrow(IllegalArgumentException::new); }
    private static TapstateException unreadable(String field) { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "alert-intent", "field", field), null); }
}
