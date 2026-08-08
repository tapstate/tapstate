package io.tapstate.app;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.RenameCase;
import io.tapstate.core.model.RenameSpec;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.StorePort;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Resolves a sink's write-side target model from the source model discovery persisted for a connection. The
 * table structure a sink creates and the key an upsert matches on come from the upstream source's discovered
 * model, not from the events flowing through - so a target table is built by reading the persisted model for
 * the pipeline's source and mapping the discovered {@link SourceTable} onto a {@link TargetTable}.
 *
 * <p>L1 shape: a pipeline reads a single source of a single table, so the resolved target is that table. When
 * the source's schema has never been discovered the target is absent, and the sink falls back to a bare table
 * id and lets the connector infer structure and keying.
 */
final class TargetModelResolver {

    private final StorePort storePort;

    TargetModelResolver(StorePort storePort) {
        this.storePort = Objects.requireNonNull(storePort, "storePort");
    }

    /**
     * Resolves the write-side target table for a pipeline's sink from the discovered model of the source it
     * reads: the selected source table looked up in its persisted model and mapped to a target table. The
     * source table travels with the target model so sink-side rename rules use the same source that supplied
     * the fields. Empty when no pipeline source has a matching discovered table.
     */
    Optional<ResolvedTarget> resolve(PipelineResource pipeline) {
        for (String sourceId : pipeline.sources()) {
            SourceResource source = StoredArtifacts.requireSource(storePort.artifacts(), sourceId);
            String table = SourceCaptureResolution.of(source).table();
            Optional<SourceTable> discovered = discoveredTable(sourceId, table);
            if (discovered.isPresent()) {
                return Optional.of(new ResolvedTarget(table, toTargetTable(discovered.get())));
            }
        }
        return Optional.empty();
    }

    record ResolvedTarget(String sourceTable, TargetTable target) {
    }

    /** The named table in the source's persisted discovery model, or empty when neither is present. */
    private Optional<SourceTable> discoveredTable(String connectionId, String table) {
        return storePort.schemas().get(connectionId)
                .map(DiscoveredSourceModel::model)
                .flatMap(model -> model.tables().stream().filter(t -> t.name().equals(table)).findFirst());
    }

    /**
     * Maps one discovered source table onto the write-side target table a sink writes: each field carries over
     * with its source-declared type, and a field named in the table's primary key is flagged so the sink keys
     * an upsert on it. The sink keys the upsert in target-field order, so the key columns lead in the source's
     * key order and the remaining fields follow in source order.
     */
    static TargetTable toTargetTable(SourceTable source) {
        List<String> primaryKey = source.primaryKey();
        List<TargetField> fields = new ArrayList<>(source.fields().size());
        for (String keyColumn : primaryKey) {
            SourceField field = field(source, keyColumn);
            fields.add(new TargetField(field.name(), field.dataType(), true));
        }
        for (SourceField field : source.fields()) {
            if (!primaryKey.contains(field.name())) {
                fields.add(new TargetField(field.name(), field.dataType(), false));
            }
        }
        return new TargetTable(source.name(), fields);
    }

    static TargetTable toTargetTable(SourceTable source, RenameSpec rename) {
        return rename(toTargetTable(source), source.name(), rename);
    }

    static TargetTable rename(TargetTable target, String sourceName, RenameSpec rename) {
        if (rename == null) {
            return target;
        }
        List<TargetField> fields = target == null ? List.of() : target.fields();
        return new TargetTable(renamedName(sourceName, rename), fields);
    }

    private static String renamedName(String sourceName, RenameSpec rename) {
        Map<String, String> explicit = rename.map();
        if (explicit != null && explicit.containsKey(sourceName)) {
            return explicit.get(sourceName);
        }
        RenameCase caseMode = rename.caseMode();
        String transformed = caseMode == null ? sourceName : switch (caseMode) {
            case UPPER -> sourceName.toUpperCase(Locale.ROOT);
            case LOWER -> sourceName.toLowerCase(Locale.ROOT);
            case CAMEL -> compoundCase(sourceName, false);
            case PASCAL -> compoundCase(sourceName, true);
        };
        return (rename.prefix() == null ? "" : rename.prefix())
                + transformed
                + (rename.suffix() == null ? "" : rename.suffix());
    }

    private static String compoundCase(String name, boolean capitalizeFirst) {
        StringBuilder result = new StringBuilder();
        StringBuilder word = new StringBuilder();
        for (int index = 0; index < name.length(); index++) {
            char current = name.charAt(index);
            if (!isAsciiAlphaNumeric(current)) {
                appendWord(result, word, capitalizeFirst);
                continue;
            }
            boolean lowerOrDigitFollowedByUpper = index > 0
                    && isAsciiLowerOrDigit(name.charAt(index - 1))
                    && isAsciiUpper(current);
            boolean acronymFollowedByWord = isAsciiUpper(current)
                    && index + 2 < name.length()
                    && isAsciiUpper(name.charAt(index + 1))
                    && isAsciiLower(name.charAt(index + 2));
            if (lowerOrDigitFollowedByUpper || acronymFollowedByWord) {
                appendWord(result, word, capitalizeFirst);
            }
            word.append(current);
        }
        appendWord(result, word, capitalizeFirst);
        return result.toString();
    }

    private static void appendWord(StringBuilder result, StringBuilder word, boolean capitalizeFirst) {
        if (word.isEmpty()) {
            return;
        }
        String lower = word.toString().toLowerCase(Locale.ROOT);
        if (result.length() > 0 || capitalizeFirst) {
            result.append(Character.toUpperCase(lower.charAt(0)));
            result.append(lower.substring(1));
        } else {
            result.append(lower);
        }
        word.setLength(0);
    }

    private static boolean isAsciiAlphaNumeric(char value) {
        return isAsciiUpper(value) || isAsciiLower(value) || value >= '0' && value <= '9';
    }

    private static boolean isAsciiUpper(char value) {
        return value >= 'A' && value <= 'Z';
    }

    private static boolean isAsciiLower(char value) {
        return value >= 'a' && value <= 'z';
    }

    private static boolean isAsciiLowerOrDigit(char value) {
        return isAsciiLower(value) || value >= '0' && value <= '9';
    }

    /** The discovered field a key column names; a key naming no discovered field is a broken source model. */
    private static SourceField field(SourceTable source, String name) {
        for (SourceField field : source.fields()) {
            if (field.name().equals(name)) {
                return field;
            }
        }
        throw new IllegalStateException(
                "primary key column '" + name + "' is not among the fields of discovered table '" + source.name() + "'");
    }
}
