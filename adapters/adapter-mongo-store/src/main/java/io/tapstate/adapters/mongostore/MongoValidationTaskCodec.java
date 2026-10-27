package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ValidationReportAdmission;
import io.tapstate.spi.store.ValidationTaskStore;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import java.time.Instant;
import java.util.*;

/** Explicit configuration encoding. Resource ids are values, never dynamic BSON field names. */
final class MongoValidationTaskCodec {
    static final int MAX_TASK_BYTES = 256 * 1024;
    static final long MAX_PIPELINE_TASK_BYTES = 16L * 1024 * 1024;
    static final long MAX_CLUSTER_TASK_BYTES = 64L * 1024 * 1024;
    static final int CONTROL_BYTES = 16 * 1024;
    static final List<String> CONTROL_FIELDS = List.of("taskGeneration", "modes", "closed", "lastMutationId", "schedulingRevision", "schedulingCommandId");
    private MongoValidationTaskCodec() {}

    record Definition(ValidationTaskSpec comparison, ValidationTaskStore.Settings settings) {}

    static Document initial(ValidationTaskStore.Create command) {
        return new Document("_id", MongoValidationAdmissionStore.taskId(command.task())).append("formatVersion", 1)
                .append("taskGeneration", command.task().generation().toString()).append("revision", 1L).append("closed", false)
                .append("modes", new Document("full", new Document("nextStartSequence", 1L)).append("incremental", new Document("nextStartSequence", 1L)))
                .append("definition", definition(command.comparison(), command.settings())).append("lastMutationId", command.commandId().toString())
                .append("schedulingRevision", 1L).append("schedulingCommandId", command.commandId().toString());
    }

    static Document update(Document previous, ValidationTaskStore.Snapshot current, ValidationTaskStore.Update command) {
        var next = new Document(previous).append("revision", current.revision() + 1).append("lastMutationId", command.commandId().toString())
                .append("definition", definition(command.comparison(), command.settings()));
        if (command.settings().incremental().firstCheckDelayMillis() != current.settings().incremental().firstCheckDelayMillis())
            next.append("schedulingRevision", current.schedulingRevision() + 1).append("schedulingCommandId", command.commandId().toString());
        return next;
    }

    static ValidationTaskStore.Snapshot snapshot(Document row, String cluster) {
        try {
            fields(row, "_id", "formatVersion", "taskGeneration", "revision", "closed", "modes", "definition", "lastMutationId", "schedulingRevision", "schedulingCommandId");
            version(row); var identity = row.get("_id", Document.class); fields(identity, "clusterId", "pipelineId", "taskId");
            if (!cluster.equals(identity.get("clusterId"))) throw unreadable();
            var task = new ValidationReportAdmission.Task(cluster, identity.getString("pipelineId"), identity.getString("taskId"), uuid(row, "taskGeneration"));
            var definition = definition(row.get("definition", Document.class)); var modes = row.get("modes", Document.class); fields(modes, "full", "incremental");
            return new ValidationTaskStore.Snapshot(task, number(row, "revision"), uuid(row, "lastMutationId"), definition.comparison(), definition.settings(),
                    number(row, "schedulingRevision"), uuid(row, "schedulingCommandId"), bool(row, "closed"), mode(modes.get("full", Document.class)), mode(modes.get("incremental", Document.class)));
        } catch (NullPointerException | ClassCastException | IllegalArgumentException | TapstateException invalid) { throw unreadable(); }
    }

    private static ValidationTaskStore.Mode mode(Document row) {
        if (row == null || !(row.keySet().equals(Set.of("nextStartSequence")) || row.keySet().equals(Set.of("nextStartSequence", "permission")))) throw unreadable();
        var permission = row.get("permission", Document.class); ValidationTaskStore.Permission current = null;
        if (permission != null) {
            fields(permission, "sequence", "phase", "identity"); var start = MongoValidationAdmissionStore.decodeIdentity(permission.get("identity", Document.class));
            if (start.sequence() != number(permission, "sequence")) throw unreadable();
            current = new ValidationTaskStore.Permission(start, ValidationTaskStore.Phase.valueOf(permission.getString("phase")));
        } else if (row.containsKey("permission")) throw unreadable();
        return new ValidationTaskStore.Mode(number(row, "nextStartSequence"), current);
    }

    static UUID uuid(Document row, String field) {
        var text = row.getString(field); var value = UUID.fromString(text);
        if (!value.toString().equals(text)) throw unreadable(); return value;
    }

    static Document definition(ValidationTaskSpec spec, ValidationTaskStore.Settings settings) {
        var full = settings.full();
        boolean keyInputsPresent = spec.mappings().stream().allMatch(mapping -> mapping.keys() != null && mapping.source().modelVersion() != null
                && mapping.target().modelVersion() != null && mapping.source().codecBinding() != null && mapping.target().codecBinding() != null);
        return new Document("formatVersion", 1).append("comparison", comparison(spec)).append("keyInputsPresent", keyInputsPresent)
                .append("full", new Document("method", full.method().name()).append("maxConcurrentTables", full.maxConcurrentTables())
                        .append("schedule", schedule(full.schedule())).append("scheduleEnabled", full.scheduleEnabled()).append("historyLimit", full.historyLimit()))
                .append("incremental", new Document("firstCheckDelayMillis", settings.incremental().firstCheckDelayMillis())
                        .append("historyLimit", settings.incremental().historyLimit()));
    }

    static Definition definition(Document row) {
        try {
            fields(row, "formatVersion", "comparison", "keyInputsPresent", "full", "incremental"); version(row);
            var full = row.get("full", Document.class); var incremental = row.get("incremental", Document.class);
            fields(full, "method", "maxConcurrentTables", "schedule", "scheduleEnabled", "historyLimit");
            fields(incremental, "firstCheckDelayMillis", "historyLimit");
            var result = new Definition(comparison(row.get("comparison", Document.class)), new ValidationTaskStore.Settings(
                    new ValidationTaskStore.Full(ValidationMethod.valueOf(full.getString("method")), integer(full, "maxConcurrentTables"),
                            schedule(full.get("schedule", Document.class)), bool(full, "scheduleEnabled"), integer(full, "historyLimit")),
                    new ValidationTaskStore.Incremental(number(incremental, "firstCheckDelayMillis"), integer(incremental, "historyLimit"))));
            result.comparison().requireFor(result.settings().full().method(), ValidationMode.FULL);
            if (!definition(result.comparison(), result.settings()).equals(row)) throw unreadable();
            return result;
        } catch (NullPointerException | ClassCastException | IllegalArgumentException | TapstateException invalid) { throw unreadable(); }
    }

    static Document comparison(ValidationTaskSpec spec) {
        var dependencies = spec.dependencyHashes().entrySet().stream()
                .map(entry -> new Document("id", entry.getKey()).append("hash", entry.getValue())).toList();
        return new Document("formatVersion", 1).append("pipelineId", spec.pipelineId()).append("mappingRevision", spec.mappingRevision())
                .append("comparisonRulesVersion", spec.comparisonRulesVersion()).append("dependencies", dependencies)
                .append("mappings", spec.mappings().stream().map(mapping -> new Document("mappingId", mapping.mappingId())
                        .append("outputKind", mapping.outputKind().name()).append("outputId", mapping.outputId())
                        .append("source", table(mapping.source())).append("target", table(mapping.target()))
                        .append("keys", mapping.keys() == null ? null : new Document("confirmationId", mapping.keys().confirmationId())
                                .append("source", keys(mapping.keys().source())).append("target", keys(mapping.keys().target())))).toList())
                .append("fingerprint", spec.fingerprint());
    }

    static ValidationTaskSpec comparison(Document row) {
        try {
            fields(row, "formatVersion", "pipelineId", "mappingRevision", "comparisonRulesVersion", "dependencies", "mappings", "fingerprint"); version(row);
            var dependencies = new TreeMap<String, String>();
            for (var value : documents(row, "dependencies", 1024)) {
                fields(value, "id", "hash");
                if (dependencies.putIfAbsent(value.getString("id"), value.getString("hash")) != null) throw unreadable();
            }
            var mappings = new ArrayList<ValidationTaskSpec.Mapping>();
            for (var value : documents(row, "mappings", ValidationTaskSpec.MAX_MAPPINGS)) {
                fields(value, "mappingId", "outputKind", "outputId", "source", "target", "keys");
                var keys = value.get("keys", Document.class); ValidationTaskSpec.Keys bound = null;
                if (keys != null) {
                    fields(keys, "confirmationId", "source", "target");
                    bound = new ValidationTaskSpec.Keys(keys.getString("confirmationId"), keys(keys, "source"), keys(keys, "target"));
                }
                mappings.add(new ValidationTaskSpec.Mapping(value.getString("mappingId"), ValidationTaskSpec.OutputKind.valueOf(value.getString("outputKind")),
                        value.getString("outputId"), table(value.get("source", Document.class)), table(value.get("target", Document.class)), bound));
            }
            var result = new ValidationTaskSpec(row.getString("pipelineId"), dependencies, number(row, "mappingRevision"), row.getString("comparisonRulesVersion"), mappings);
            if (!comparison(result).equals(row)) throw unreadable();
            return result;
        } catch (NullPointerException | ClassCastException | IllegalArgumentException | TapstateException invalid) { throw unreadable(); }
    }

    static Document run(ValidationRunSpec spec) {
        return new Document("formatVersion", 1).append("mode", spec.mode().name()).append("method", spec.method().name())
                .append("comparison", comparison(spec.task())).append("fingerprint", spec.fingerprint());
    }

    static ValidationRunSpec run(Document row) {
        try {
            fields(row, "formatVersion", "mode", "method", "comparison", "fingerprint"); version(row);
            var spec = new ValidationRunSpec(comparison(row.get("comparison", Document.class)), ValidationMode.valueOf(row.getString("mode")),
                    ValidationMethod.valueOf(row.getString("method")));
            if (!run(spec).equals(row)) throw unreadable(); return spec;
        } catch (NullPointerException | ClassCastException | IllegalArgumentException | TapstateException invalid) { throw unreadable(); }
    }

    private static Document table(ValidationTaskSpec.Table table) {
        return new Document("nodeId", table.nodeId()).append("sourceId", table.sourceId()).append("database", table.database()).append("schema", table.schema())
                .append("table", table.table()).append("modelVersion", table.modelVersion()).append("codecBinding", table.codecBinding());
    }
    private static ValidationTaskSpec.Table table(Document row) {
        fields(row, "nodeId", "sourceId", "database", "schema", "table", "modelVersion", "codecBinding");
        return new ValidationTaskSpec.Table(row.getString("nodeId"), row.getString("sourceId"), row.getString("database"), row.getString("schema"),
                row.getString("table"), row.getString("modelVersion"), row.getString("codecBinding"));
    }
    private static List<Document> keys(List<ValidationKeyBinding.Component> keys) {
        return keys.stream().map(key -> new Document("path", key.path()).append("domain", key.domain().name())
                .append("numericPolicy", key.numericPolicy().name()).append("logicalTypes", key.logicalTypes().stream().sorted().toList())).toList();
    }
    private static List<ValidationKeyBinding.Component> keys(Document row, String name) {
        var result = new ArrayList<ValidationKeyBinding.Component>();
        for (var value : documents(row, name, ValidationKey.MAX_COMPONENTS)) {
            fields(value, "path", "domain", "numericPolicy", "logicalTypes");
            var path = strings(value, "path", 32); var types = strings(value, "logicalTypes", 32);
            if (new HashSet<>(types).size() != types.size()) throw unreadable();
            result.add(new ValidationKeyBinding.Component(path, ValidationKey.Kind.valueOf(value.getString("domain")), Set.copyOf(types),
                    ValidationKeyBinding.NumericPolicy.valueOf(value.getString("numericPolicy"))));
        }
        return result;
    }
    private static Document schedule(ValidationSchedule schedule) {
        if (schedule == null) return null;
        if (schedule instanceof ValidationSchedule.Interval interval) return new Document("kind", "INTERVAL").append("every", interval.every())
                .append("unit", interval.unit().name()).append("anchor", interval.anchor().toString()).append("zoneId", interval.zoneId())
                .append("rulesVersion", ValidationSchedule.RULES_VERSION);
        var cron = (ValidationSchedule.Cron) schedule;
        return new Document("kind", "CRON").append("expression", cron.expression()).append("zoneId", cron.zoneId()).append("rulesVersion", ValidationSchedule.RULES_VERSION);
    }
    private static ValidationSchedule schedule(Document row) {
        if (row == null) return null;
        if (!ValidationSchedule.RULES_VERSION.equals(row.get("rulesVersion"))) throw unreadable();
        if ("INTERVAL".equals(row.get("kind"))) {
            fields(row, "kind", "every", "unit", "anchor", "zoneId", "rulesVersion");
            return new ValidationSchedule.Interval(number(row, "every"), ValidationSchedule.Unit.valueOf(row.getString("unit")),
                    Instant.parse(row.getString("anchor")), row.getString("zoneId"));
        }
        fields(row, "kind", "expression", "zoneId", "rulesVersion");
        if (!"CRON".equals(row.get("kind"))) throw unreadable();
        return new ValidationSchedule.Cron(row.getString("expression"), row.getString("zoneId"));
    }

    static int bsonBytes(Document value) {
        try (var buffer = new BasicOutputBuffer(); var writer = new BsonBinaryWriter(buffer)) {
            new DocumentCodec().encode(writer, value, EncoderContext.builder().build()); return buffer.getSize();
        }
    }
    /** The fixed definition reserves the complete admission envelope before any first run. */
    static long reservedBytes(Document row) {
        var body = new Document(row); CONTROL_FIELDS.forEach(body::remove);
        return Math.max(bsonBytes(row), (long) bsonBytes(body) + CONTROL_BYTES - 5);
    }
    static Document reservedBytesExpression() {
        var body = new Document("$arrayToObject", new Document("$filter", new Document("input", new Document("$objectToArray", "$$ROOT"))
                .append("as", "field").append("cond", new Document("$eq", List.of(new Document("$in", List.of("$$field.k", CONTROL_FIELDS)), false)))));
        return new Document("$toLong", new Document("$max", List.of(new Document("$bsonSize", "$$ROOT"),
                new Document("$add", List.of(new Document("$bsonSize", body), CONTROL_BYTES - 5)))));
    }
    private static List<Document> documents(Document row, String field, int maximum) {
        var values = row.getList(field, Document.class); if (values == null || values.size() > maximum) throw unreadable(); return values;
    }
    private static List<String> strings(Document row, String field, int maximum) {
        var values = row.getList(field, String.class); if (values == null || values.size() > maximum) throw unreadable(); return values;
    }
    private static int integer(Document row, String field) { if (!(row.get(field) instanceof Integer value)) throw unreadable(); return value; }
    private static long number(Document row, String field) { if (!(row.get(field) instanceof Long value)) throw unreadable(); return value; }
    private static boolean bool(Document row, String field) { if (!(row.get(field) instanceof Boolean value)) throw unreadable(); return value; }
    private static void version(Document row) { if (!Integer.valueOf(1).equals(row.get("formatVersion"))) throw unreadable(); }
    private static void fields(Document row, String... keys) { if (row == null || !row.keySet().equals(Set.of(keys))) throw unreadable(); }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "validation", "field", "task-definition"), null); }
}
