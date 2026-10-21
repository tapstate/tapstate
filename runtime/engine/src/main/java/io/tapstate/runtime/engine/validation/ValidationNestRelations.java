package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.validation.ValidationError;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Compiles current relation declarations from an already parsed Nest model, independently of the
 * production topology, state or renderer. This is not runnable admission: logical key types, native
 * query encodings/indexes, confirmed target field bindings and resource reservations remain required.
 * Missing identity metadata is explicit, never a claim that source records are unique or keyless.
 * The caller admits the input model/configuration allocations separately from this retained plan.
 */
public final class ValidationNestRelations {
    public enum Direction { GATHER, REFERENCE }
    public record Metadata(List<String> primaryKey, List<List<String>> uniqueIndexes) {
        public Metadata {
            primaryKey = fields(primaryKey, "metadata.primaryKey", true);
            if (uniqueIndexes == null || uniqueIndexes.size() > 256) throw invalid("metadata.uniqueIndexes");
            uniqueIndexes = uniqueIndexes.stream().map(index -> fields(index, "metadata.uniqueIndex", false)).toList();
        }
    }
    public record Pair(String child, String parent) {}
    public record Relation(String alias, String parentAlias, EmbedAs shape, Optional<String> outputPath,
                           List<String> treePath, Direction direction, List<Pair> join,
                           Optional<List<String>> rowKey, Optional<List<String>> arrayKey, List<Relation> children) {
        public Relation {
            treePath = List.copyOf(treePath); join = List.copyOf(join); children = List.copyOf(children);
        }
    }
    public record Plan(String node, String rootAlias, Optional<List<String>> rootKey, List<Relation> children, long retainedBytes) {
        public Plan { children = List.copyOf(children); }
    }
    public record Limits(int nodes, int depth, long bytes) {
        public Limits {
            if (nodes < 1 || nodes > 256 || depth < 1 || depth > 32 || bytes < 1 || bytes > 4L * 1024 * 1024) {
                throw invalid("nestRelations.limits");
            }
        }
    }

    private final String node;
    private final Set<String> aliases;
    private final Map<String, Metadata> metadata;
    private final Limits limits;
    private int nodes;
    private long bytes;
    private ValidationNestRelations(String node, Set<String> aliases, Map<String, Metadata> metadata, Limits limits) {
        this.node = name(node, "nestRelations.node");
        this.aliases = aliases;
        this.metadata = metadata;
        this.limits = limits;
    }

    /** Metadata must be pinned to the original definition/model version, not inferred from sample rows. */
    public static Plan compile(String node, NestRoot root, Set<String> aliases, Map<String, Metadata> metadata, Limits limits) {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(aliases, "aliases");
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(limits, "limits");
        if (aliases.isEmpty() || aliases.size() > 256 || metadata.size() > 256 || !aliases.containsAll(metadata.keySet())) {
            throw invalid("nestRelations.aliases");
        }
        for (String alias : aliases) name(alias, "nestRelations.alias");
        var compiler = new ValidationNestRelations(node, Set.copyOf(aliases), Map.copyOf(metadata), limits);
        compiler.alias(root.from());
        compiler.charge(256L + 2L * node.length() + 2L * root.from().length());
        var key = compiler.key(root.from(), root.key());
        var children = compiler.children(root.from(), key, List.of(), root.embed(), 1);
        return new Plan(node, root.from(), key, children, compiler.bytes);
    }

    private List<Relation> children(String parent, Optional<List<String>> parentKey, List<String> treePath, List<Embed> embeds, int depth) {
        if (embeds == null || embeds.isEmpty()) return List.of();
        if (depth > limits.depth() || embeds.size() > limits.nodes() - nodes) throw limit("nodesOrDepth");
        var paths = new ArrayList<String>();
        var identities = new HashSet<String>();
        var result = new ArrayList<Relation>();
        for (Embed embed : embeds) {
            Objects.requireNonNull(embed, "embed");
            if (++nodes > limits.nodes()) throw limit("nodes");
            alias(embed.from());
            String segment = embed.as() == EmbedAs.FLAT ? "$flat[" + embed.from() + "]" : name(embed.path(), "nestRelations.path");
            if (!identities.add(segment)) throw unsupported(segment, "nest.duplicatePath");
            if (embed.as() != EmbedAs.FLAT) {
                for (String existing : paths) if (overlap(existing, segment)) throw unsupported(segment, "nest.overlappingPaths");
                paths.add(segment);
            }
            var on = new LinkedHashMap<String, String>();
            if (embed.on().isEmpty() || embed.on().size() > 32) throw unsupported(segment, "nest.onArity");
            for (var pair : embed.on().entrySet()) {
                on.put(name(pair.getKey(), "nestRelations.childField"), name(pair.getValue(), "nestRelations.parentField"));
                charge(96L + 2L * pair.getKey().length() + 2L * pair.getValue().length());
            }
            var rowKey = key(embed.from(), embed.key());
            Direction direction = direction(parentKey, rowKey, on, segment);
            var ordered = new ArrayList<Pair>();
            if (direction == Direction.REFERENCE) {
                for (String field : rowKey.orElseThrow()) ordered.add(new Pair(field, on.get(field)));
            } else if (parentKey.isPresent()) {
                for (String field : parentKey.orElseThrow()) {
                    String child = on.entrySet().stream().filter(pair -> pair.getValue().equals(field)).findFirst().orElseThrow().getKey();
                    ordered.add(new Pair(child, field));
                }
            } else {
                // Equality conjunctions have no ordering semantics; fix one order when no parent key exists.
                for (String field : on.keySet().stream().sorted().toList()) ordered.add(new Pair(field, on.get(field)));
            }
            var path = new ArrayList<>(treePath); path.add(segment);
            var arrayKey = embed.as() == EmbedAs.ARRAY
                    ? embed.arrayKey() == null ? rowKey : Optional.of(fields(embed.arrayKey(), "nestRelations.arrayKey", false))
                    : Optional.<List<String>>empty();
            charge(384L + 2L * embed.from().length() + 2L * parent.length() + 2L * segment.length() + 8L * path.size());
            arrayKey.ifPresent(value -> chargeFields(value));
            var nested = children(embed.from(), rowKey, path, embed.embed(), depth + 1);
            result.add(new Relation(embed.from(), parent, embed.as(), Optional.ofNullable(embed.path()), path,
                    direction, ordered, rowKey, arrayKey, nested));
        }
        return result;
    }

    private Direction direction(Optional<List<String>> parentKey, Optional<List<String>> childKey, Map<String, String> on, String path) {
        if (parentKey.isEmpty()) return Direction.GATHER;
        boolean parentMatches = parentKey.orElseThrow().size() == on.size()
                && new HashSet<>(parentKey.orElseThrow()).equals(new HashSet<>(on.values()));
        if (parentMatches) return Direction.GATHER;
        if (childKey.isEmpty()) throw unsupported(path, "nest.rowKeyUnavailable");
        if (childKey.orElseThrow().size() == on.size() && new HashSet<>(childKey.orElseThrow()).equals(on.keySet())) return Direction.REFERENCE;
        throw unsupported(path, "nest.onDoesNotIdentifyEitherSide");
    }
    private Optional<List<String>> key(String alias, List<String> explicit) {
        Optional<List<String>> result;
        if (explicit != null) result = Optional.of(fields(explicit, "nestRelations.key", false));
        else {
            Metadata found = metadata.get(alias);
            result = found == null ? Optional.empty() : !found.primaryKey().isEmpty() ? Optional.of(found.primaryKey())
                    : found.uniqueIndexes().size() == 1 ? Optional.of(found.uniqueIndexes().getFirst()) : Optional.empty();
        }
        result.ifPresent(this::chargeFields);
        return result;
    }
    private void alias(String alias) {
        name(alias, "nestRelations.alias");
        if (!aliases.contains(alias)) throw unsupported(alias, "nest.unknownAlias");
    }
    private void chargeFields(List<String> fields) { for (String field : fields) charge(40L + 2L * field.length()); }
    private void charge(long amount) { if (amount > limits.bytes() - bytes) throw limit("bytes"); bytes += amount; }
    private static boolean overlap(String left, String right) {
        return left.equals(right) || left.startsWith(right + ".") || right.startsWith(left + ".");
    }
    private static List<String> fields(List<String> fields, String field, boolean allowEmpty) {
        if (fields == null || fields.size() > 32 || !allowEmpty && fields.isEmpty()) throw invalid(field);
        var distinct = new HashSet<String>();
        for (String value : fields) if (!distinct.add(name(value, field))) throw invalid(field);
        return List.copyOf(fields);
    }
    private static String name(String value, String field) {
        if (value == null || value.isBlank() || value.length() > 512) throw invalid(field);
        return value;
    }
    private TapstateException unsupported(String field, String rule) {
        return new TapstateException(ValidationError.RULE_UNSUPPORTED, Map.of("node", node, "field", field, "rule", rule), null);
    }
    private static TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null); }
    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "nestRelations." + dimension), null);
    }
}
