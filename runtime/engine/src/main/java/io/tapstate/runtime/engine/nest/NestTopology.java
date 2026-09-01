package io.tapstate.runtime.engine.nest;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.WriteMode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * A nest tree compiled into the graph that runs it: one resolver vertex per embed that has children of
 * its own, one assembler, and the edges between them. Everything the running job needs to know about
 * shape - how many vertices, what each is keyed by, which edge carries which stream, how far a row
 * travels - is decided here and never re-decided while running.
 *
 * <p>A vertex per non-leaf embed rather than per level is what makes branching safe. Two embeds side by
 * side would otherwise share a map, and two tables with their own auto-increment keys do not merely
 * collide occasionally - their low id ranges overlap wholesale, so elements land in the wrong document
 * with nothing reported. Separate vertices make that unrepresentable rather than unlikely.
 *
 * <p>The tree is checked while it is compiled, and everything an author can get wrong is refused with a
 * code. A tree that compiles is one the runtime can assume is well formed.
 *
 * @param foldingAllowed whether two versions of one document may go out as one. False under an append
 *        root, where every send is a new record and merging two versions loses one rather than saving a
 *        write. It is read off the tree here because that is where the root's mode is known, and it
 *        decides how the assembler sends rather than what it assembles.
 */
public record NestTopology(List<NestVertex> vertices, List<NestStream> streams, List<EmbedSlot> slots,
        boolean foldingAllowed) implements Serializable {

    /**
     * How many resolver vertices one nest may compile to. Each takes a thread of its own rather than
     * sharing the cooperative pool, so the count is a real resource and not just a number. Provisional
     * until the thread budget is settled against the state layer's own accounting.
     */
    public static final int DEFAULT_RESOLVER_VERTEX_LIMIT = 32;

    /** The name the root's own namespace answers to, where an embed would put its path. */
    private static final String ROOT_NAMESPACE = "$root";

    /** The separator between path segments in a rendered path, and so in a vertex and map name. */
    private static final String PATH_SEPARATOR = ".";

    public NestTopology {
        vertices = List.copyOf(vertices);
        streams = List.copyOf(streams);
        slots = List.copyOf(slots);
    }

    /** Compiles the tree against the default vertex limit. */
    public static NestTopology compile(String pipelineId, String nodeId, TransformBody.Nest nest,
            Function<String, NestTable> tables) {
        return compile(pipelineId, nodeId, nest, tables, DEFAULT_RESOLVER_VERTEX_LIMIT);
    }

    /**
     * Compiles the tree, refusing it with a code when it is not well formed. A root with no embeds at all
     * compiles to nothing: it would pay for a map, a vertex and a thread while assembling no document, so
     * it degenerates to a passthrough and the caller wires the root stream straight on.
     */
    public static NestTopology compile(String pipelineId, String nodeId, TransformBody.Nest nest,
            Function<String, NestTable> tables, int resolverVertexLimit) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(tables, "tables");
        NestRoot root = nest.root();
        List<Embed> declared = childrenOf(root.embed());
        if (declared.isEmpty()) {
            // A passthrough assembles nothing and so sends nothing of its own: how a document would be
            // folded is not a question it has.
            return new NestTopology(List.of(), List.of(), List.of(), true);
        }

        // The root is a level like any other: what identifies one of its rows is asked of the same four
        // places, so a root over a table that declares a key need not repeat it.
        List<String> rootKey = keyOf(root.from(), root.key(), ROOT_NAMESPACE, tables);
        List<Node> top = new ArrayList<>();
        for (Embed embed : declared) {
            top.add(node(embed, List.of(), root.from(), rootKey));
        }
        List<Node> all = flatten(top);

        checkPaths(top);
        for (Node node : all) {
            checkPaths(node.children());
        }
        // Which way each embed points, before anything asks: the readers below all branch on it, and an
        // embed the level points at is not one of the things naming that level's identity.
        for (Node node : all) {
            node.referenced(referenced(node, tables));
        }
        List<String> rootIdentity = identityOf(ROOT_NAMESPACE, rootKey, claimants(top));
        for (Node node : all) {
            if (!node.children().isEmpty()) {
                List<Node> claiming = claimants(node.children());
                // A level every child points at is identified by what those children agree on. A level
                // none of them points at is identified by what identifies one of its own rows: it is
                // still a level, it just has nobody naming it from below.
                node.identity(claiming.isEmpty()
                        ? resolveArrayKey(node, tables)
                        : identityOf(render(node.pathId()), null, claiming));
            }
        }
        for (Node node : all) {
            node.arrayKey(resolveArrayKey(node, tables));
        }
        checkAppendMode(root, all);
        checkVertexCount(all, resolverVertexLimit);

        return assemble(pipelineId, nodeId, root, rootIdentity, top, all, tables);
    }

    /**
     * Every path this tree addresses state by: one per embed, plus the root's own. It is the shape of the
     * tree as the state layer sees it - two trees with the same set reach each other's entries and two
     * with different sets reach none of them.
     *
     * <p>It is deliberately wider than the set of namespaces. A namespace exists only where an embed has
     * children of its own, but a leaf's elements are filed inside its parent's state under the leaf's own
     * path, so renaming a leaf abandons what was stored just as thoroughly while every map keeps its name.
     * Comparing namespaces would see nothing at all in that case, which is the case an author is most
     * likely to reach for.
     */
    public Set<String> statePaths() {
        Set<String> paths = new LinkedHashSet<>();
        for (NestStream stream : streams) {
            paths.add(render(stream.pathId()));
        }
        return paths;
    }

    /**
     * Every namespace this tree keeps state in: one per vertex, named the same way the vertex asks for its
     * map. A tree being taken down lets go of its state by naming these, which is why they are taken from
     * the compiled vertices rather than from anything remembered - what a vertex wrote is by definition
     * under the name that vertex was compiled with.
     *
     * <p>Narrower than {@link #statePaths()}, and deliberately so: a leaf has no namespace of its own, its
     * elements being filed inside its parent's state. Dropping the parent's namespace takes them with it,
     * so a set that named leaves would name places nothing was ever stored.
     *
     * <p>Two per vertex rather than one: the state itself, and the area where subtrees sit while they move
     * between documents. The second is state as much as the first, and a set that named only the first
     * would leave a move that was in flight behind whenever a pipeline was taken down.
     */
    public Set<String> stateNamespaces() {
        Set<String> namespaces = new LinkedHashSet<>();
        for (NestVertex vertex : vertices) {
            namespaces.add(vertex.mapName());
            // Where a subtree sits while it moves between documents. Named here as well because it is state
            // like any other: left out, a tree taken down would leave the rows of a move that was in flight
            // behind it, under a name nothing would ever look at again.
            namespaces.add(vertex.parkingMapName());
        }
        return namespaces;
    }

    /** Whether the tree assembles nothing, so the node is wired as a passthrough with no state at all. */
    public boolean isPassthrough() {
        return vertices.isEmpty();
    }

    /** The resolver vertices, ordered so every cascade's source comes before the vertex it feeds. */
    public List<NestVertex> resolvers() {
        return vertices.isEmpty() ? List.of() : vertices.subList(0, vertices.size() - 1);
    }

    /** The vertex that holds whole documents. Asking a passthrough for one is a wiring bug. */
    public NestVertex assembler() {
        if (vertices.isEmpty()) {
            throw new IllegalStateException("a passthrough nest has no assembler");
        }
        return vertices.get(vertices.size() - 1);
    }

    /** The vertex serving the embed at {@code pathId}, the assembler for the empty path. */
    public NestVertex vertexAt(List<String> pathId) {
        for (NestVertex vertex : vertices) {
            if (vertex.pathId().equals(pathId)) {
                return vertex;
            }
        }
        throw new IllegalArgumentException("no vertex serves " + pathId);
    }

    /** The stream feeding the embed at {@code pathId}, the root's own stream for the empty path. */
    public NestStream streamAt(List<String> pathId) {
        for (NestStream stream : streams) {
            if (stream.pathId().equals(pathId)) {
                return stream;
            }
        }
        throw new IllegalArgumentException("no stream feeds " + pathId);
    }

    private static NestTopology assemble(String pipelineId, String nodeId, NestRoot root,
            List<String> rootIdentity, List<Node> top, List<Node> all, Function<String, NestTable> tables) {
        Map<List<String>, List<String>> identities = new LinkedHashMap<>();
        identities.put(List.of(), rootIdentity);
        for (Node node : all) {
            identities.put(node.pathId(), node.identity());
        }
        List<NestVertex> vertices = new ArrayList<>();
        List<NestStream> streams = new ArrayList<>();
        for (Node node : all) {
            if (!node.children().isEmpty()) {
                vertices.add(vertexOf(pipelineId, nodeId, node, identities.get(node.parentPathId()), tables));
            }
        }
        String assemblerName = vertexName(nodeId, List.of());
        vertices.add(new NestVertex(List.of(), assemblerName,
                mapName(pipelineId, nodeId, List.of()), rootIdentity, List.of(),
                edgesOf(root.from(), List.of(), List.of(), rootIdentity, top,
                        Boolean.TRUE.equals(root.trackKeyChanges()), tables)));

        streams.add(new NestStream(root.from(), List.of(), 0, assemblerName, null));
        for (Node node : all) {
            boolean leaf = node.children().isEmpty();
            int depth = node.pathId().size();
            String entry = leaf
                    ? vertexName(nodeId, node.parentPathId())
                    : vertexName(nodeId, node.pathId());
            streams.add(new NestStream(node.embed().from(), node.pathId(), leaf ? depth - 1 : depth,
                    entry, node.arrayKey()));
        }
        return new NestTopology(vertices, streams, slotsOf(top),
                !WriteMode.APPEND.yaml().equals(root.mode()));
    }

    private static NestVertex vertexOf(String pipelineId, String nodeId, Node node, List<String> parentIdentity,
            Function<String, NestTable> tables) {
        return new NestVertex(node.pathId(), vertexName(nodeId, node.pathId()),
                mapName(pipelineId, nodeId, node.pathId()), node.identity(),
                joinFields(node.embed(), parentIdentity),
                edgesOf(node.embed().from(), node.pathId(), node.arrayKey(), node.identity(), node.children(),
                        Boolean.TRUE.equals(node.embed().trackKeyChanges()), tables));
    }

    /**
     * The edges into one vertex: its own rows first, then one per child embed. A leaf child arrives
     * directly, keyed by the join field it carries; a child with children of its own arrives cascading
     * from its own vertex, already stamped. The count is one plus the number of children - two only when
     * there happens to be exactly one child.
     */
    private static List<NestInbound> edgesOf(String ownAlias, List<String> ownPathId, List<String> ownElementKey,
            List<String> identity, List<Node> children, boolean ownTracksKeyChanges,
            Function<String, NestTable> tables) {
        List<NestInbound> edges = new ArrayList<>();
        edges.add(new NestInbound(0, ownAlias, ownPathId, identity, ownElementKey, ownTracksKeyChanges,
                tableBehind(ownAlias, ownTracksKeyChanges, tables)));
        for (Node child : children) {
            if (child.referenced()) {
                // A child the level points at does not arrive here. Its rows are not grouped under this
                // level - one of them can sit under thousands of levels at once - so there is no field
                // pairing to key an edge by, and routing the document to it would be routing it to every
                // holder of the same reference. It is read by key where the document is rendered.
                continue;
            }
            boolean tracked = Boolean.TRUE.equals(child.embed().trackKeyChanges());
            edges.add(child.children().isEmpty()
                    ? new NestInbound(edges.size(), child.embed().from(), child.pathId(),
                            joinFields(child.embed(), identity), child.arrayKey(), tracked,
                            tableBehind(child.embed().from(), tracked, tables))
                    : new NestInbound(edges.size(), null, child.pathId(), List.of(), List.of()));
        }
        // Appended after every edge above, never interleaved: an ordinal is how a processor tells one kind
        // of arrival from another, so inserting one would renumber the rest and silently point every later
        // edge at the wrong handling.
        //
        // Only the ones that arrive as rows of a source get a twin. A cascading edge carries changes another
        // vertex already routed, so where they were is already settled and there is nothing to re-key.
        for (NestInbound edge : List.copyOf(edges)) {
            if (edge.tracksKeyChanges()) {
                edges.add(NestInbound.departuresOf(edges.size(), edge));
            }
        }
        return edges;
    }

    /**
     * The source table an edge's alias stands for, resolved only where key changes are tracked. Looked up
     * for those alone because it is needed for one thing - saying which table has to start sending before
     * images - and because an alias that resolves to no table is tolerated elsewhere whenever the tree
     * declares what would otherwise be read off it.
     */
    private static String tableBehind(String alias, boolean tracked, Function<String, NestTable> tables) {
        if (!tracked) {
            return null;
        }
        NestTable table = tables.apply(alias);
        if (table == null) {
            throw new IllegalStateException("nest alias '" + alias + "' resolves to no table");
        }
        return table.name();
    }

    /**
     * The child-side fields carrying this embed's join key, ordered to match the fields its parent is
     * partitioned by, so a composite key reads off positionally on every edge alike.
     */
    private static List<String> joinFields(Embed embed, List<String> identity) {
        List<String> fields = new ArrayList<>();
        for (String parentField : identity) {
            for (Map.Entry<String, String> pair : embed.on().entrySet()) {
                if (pair.getValue().equals(parentField)) {
                    fields.add(pair.getKey());
                    break;
                }
            }
        }
        return fields;
    }

    /**
     * The field every child of one parent points at, which is that parent's identity. At the root the key
     * the root declares joins the comparison, because the assembler is partitioned by it and an embed
     * pointing anywhere else names a column the assembler has no way to look up.
     */
    private static List<String> identityOf(String owner, List<String> rootKey, List<Node> children) {
        List<List<String>> claims = new ArrayList<>();
        if (rootKey != null) {
            claims.add(rootKey);
        }
        for (Node child : children) {
            claims.add(List.copyOf(new LinkedHashSet<>(child.embed().on().values())));
        }
        List<String> chosen = claims.get(0);
        Set<String> expected = new LinkedHashSet<>(chosen);
        Set<String> named = new TreeSet<>();
        boolean agreed = true;
        for (List<String> claim : claims) {
            named.addAll(claim);
            agreed &= new LinkedHashSet<>(claim).equals(expected);
        }
        if (!agreed) {
            throw new TapstateException(NestError.SIBLING_EMBEDS_TARGET_DIFFERENT_PARENT_KEYS,
                    Map.of("embedPath", owner, "fields", String.join(", ", named)), null);
        }
        return chosen;
    }

    /**
     * Refuses a level whose children join onto a column that does not identify its rows. What a child names
     * is not a column to look the parent up by; it is the declaration of what the level is keyed on, and the
     * level is partitioned by it. Name one many rows share and they collapse into a single identity, taking
     * whichever row got there and leaving the rest with nothing - quietly, since every count stays at zero.
     *
     * <p>Siblings catch this by disagreeing with each other. An only child has nobody to disagree with, which
     * is why the level's own key has to be asked. Only a declared key can say whether a column identifies a
     * row, so a level whose table declares none is left alone rather than guessed at.
     */
    /**
     * The children that say something about the identity of the level they hang under. A child the level
     * points at says nothing about it: the column is a reference the level's rows carry, and reading it as
     * the level's identity would regroup those rows by whatever they happen to refer to.
     */
    private static List<Node> claimants(List<Node> children) {
        List<Node> claiming = new ArrayList<>();
        for (Node child : children) {
            if (!child.referenced()) {
                claiming.add(child);
            }
        }
        return claiming;
    }

    /**
     * Which side of an embed's join carries the other's identity, read off the keys rather than declared.
     *
     * <p>The two directions are written identically - a pair of column names either way - so the question
     * cannot be answered from the pair alone. It can be answered from what identifies a row: the side that
     * names its own table's key is the side being pointed at, because a column that identifies a row names
     * one row, and a column that does not names however many share it.
     *
     * <p>Both sides being keys is a one-to-one join read either way. It renders the same document both
     * times, so the existing direction is taken and nothing about such a tree changes. Neither side being
     * a key is the case with no answer at all, and it is refused: the tree would otherwise assemble
     * documents grouped on a column many rows share, with every count where it should be and the contents
     * wrong.
     */
    private static boolean referenced(Node node, Function<String, NestTable> tables) {
        Set<String> childSide = new LinkedHashSet<>(node.embed().on().keySet());
        Set<String> parentSide = new LinkedHashSet<>(node.embed().on().values());
        List<String> parentKey =
                keyOf(node.parentAlias(), node.parentKey(), render(node.parentPathId()), tables);
        // The parent side alone settles the existing direction, so the embed's own key is not asked for
        // unless it has to be. Asking anyway would refuse a tree that is already answered - over a stream
        // nothing discovered, say - for want of something the answer does not depend on.
        if (parentSide.equals(new LinkedHashSet<>(parentKey))) {
            return false;
        }
        if (childSide.equals(new LinkedHashSet<>(rowKeyOf(node, tables)))) {
            return true;
        }
        throw new TapstateException(NestError.EMBED_TARGET_NOT_PARENT_KEY,
                Map.of("embedPath", render(node.pathId()),
                        "fields", String.join(", ", parentSide),
                        "parentKey", String.join(", ", parentKey)), null);
    }

    /**
     * Refuses two embeds under one parent that claim the same place in the document, or one that claims a
     * place inside the other's. Either way one would silently overwrite the other, and either way their
     * paths from the root would render the same - which is also what keeps a rendered path unique.
     */
    private static void checkPaths(List<Node> children) {
        for (int i = 0; i < children.size(); i++) {
            for (int j = i + 1; j < children.size(); j++) {
                List<String> a = segments(children.get(i).embed().path());
                List<String> b = segments(children.get(j).embed().path());
                int common = Math.min(a.size(), b.size());
                if (a.subList(0, common).equals(b.subList(0, common))) {
                    throw new TapstateException(NestError.EMBED_PATH_CONFLICT,
                            Map.of("path", String.join(PATH_SEPARATOR, a.size() <= b.size() ? a : b),
                                    "embedPathA", render(children.get(i).pathId()),
                                    "embedPathB", render(children.get(j).pathId())), null);
                }
            }
        }
    }

    /**
     * The element identity for one embed: what it declares, or the key its table declares when it declares
     * none. A table the caller cannot resolve at all is a wiring bug rather than an authoring error, and
     * bare-throws; a table that resolves but declares no key is the author's to fix.
     */
    /**
     * What tells one element of this embed apart from the others in the same array. It is allowed to be
     * unique only within that array - an order's line numbers are the everyday case - so it is asked for
     * separately from the row identity, and falls back to it when the author wrote nothing.
     */
    private static List<String> resolveArrayKey(Node node, Function<String, NestTable> tables) {
        List<String> declared = node.embed().arrayKey();
        if (declared != null && !declared.isEmpty()) {
            return declared;
        }
        return rowKeyOf(node, tables);
    }

    /** What identifies one row of this embed's stream, wherever else that row may also appear. */
    private static List<String> rowKeyOf(Node node, Function<String, NestTable> tables) {
        return keyOf(node.embed().from(), node.embed().key(), render(node.pathId()), tables);
    }

    /**
     * What identifies one row of a level, asked of four places in order: the key the level declares, the
     * table's primary key, its one unique index, and then nothing - which refuses the tree rather than
     * carrying an unknown identity into the assembly.
     *
     * <p>A declared key is taken as written and the schema is never consulted to second-guess it. A
     * business identity and a physical primary key are allowed to differ, and only the author knows
     * whether they do here; a check would have to call one of them wrong without being able to tell.
     *
     * <p>Two unique indexes are refused rather than resolved. Both identify a row equally well, so any
     * rule for choosing settles the identity of a whole level on which index the source reported first -
     * silently, and differently on a catalog that changes. The refusal names both and the level, because
     * the rung that settles it is the first one and it is one line to write.
     */
    private static List<String> keyOf(String alias, List<String> declared, String owner,
            Function<String, NestTable> tables) {
        if (declared != null && !declared.isEmpty()) {
            return declared;
        }
        NestTable table = tables.apply(alias);
        if (table == null) {
            throw new IllegalStateException("nest alias '" + alias + "' resolves to no table");
        }
        if (!table.primaryKey().isEmpty()) {
            return table.primaryKey();
        }
        List<List<String>> unique = table.uniqueIndexes();
        if (unique.size() == 1) {
            return unique.get(0);
        }
        if (unique.size() > 1) {
            List<String> candidates = new ArrayList<>(unique.size());
            for (List<String> index : unique) {
                candidates.add("(" + String.join(", ", index) + ")");
            }
            throw new TapstateException(NestError.KEY_AMBIGUOUS,
                    Map.of("embedPath", owner, "table", table.name(),
                            "candidates", String.join(", ", candidates)), null);
        }
        throw new TapstateException(NestError.ARRAY_KEY_UNRESOLVABLE,
                Map.of("embedPath", owner, "table", table.name()), null);
    }

    /**
     * Refuses an append-only root under a tree that tracks structural key changes. Moving a subtree has to
     * hold emissions back until it lands, and holding them back is the one thing append-only forbids.
     */
    private static void checkAppendMode(NestRoot root, List<Node> all) {
        if (!WriteMode.APPEND.yaml().equals(root.mode())) {
            return;
        }
        for (Node node : all) {
            if (Boolean.TRUE.equals(node.embed().trackKeyChanges())) {
                throw new TapstateException(NestError.APPEND_MODE_CONFLICTS_WITH_KEY_TRACKING,
                        Map.of("rootAlias", root.from(), "embedPath", render(node.pathId())), null);
            }
        }
        if (Boolean.TRUE.equals(root.trackKeyChanges())) {
            throw new TapstateException(NestError.APPEND_MODE_CONFLICTS_WITH_KEY_TRACKING,
                    Map.of("rootAlias", root.from(), "embedPath", ROOT_NAMESPACE), null);
        }
    }

    private static void checkVertexCount(List<Node> all, int limit) {
        int resolvers = 0;
        for (Node node : all) {
            if (!node.children().isEmpty()) {
                resolvers++;
            }
        }
        if (resolvers > limit) {
            throw new TapstateException(NestError.RESOLVER_VERTEX_LIMIT_EXCEEDED,
                    Map.of("vertices", resolvers, "limit", limit), null);
        }
    }

    /**
     * The shape the assembler renders documents into: which field each embed occupies and whether an
     * absent one shows as an empty array or not at all. It is a property of the declared tree and not of
     * the data, which is why it is settled here rather than remembered per document.
     */
    private static List<EmbedSlot> slotsOf(List<Node> nodes) {
        List<EmbedSlot> slots = new ArrayList<>();
        for (Node node : nodes) {
            Embed embed = node.embed();
            slots.add(new EmbedSlot(embed.path(), embed.as(), slotsOf(node.children())));
        }
        return slots;
    }

    private static String vertexName(String nodeId, List<String> pathId) {
        return pathId.isEmpty() ? "nest:" + nodeId : "nest:" + nodeId + ":" + render(pathId);
    }

    private static String mapName(String pipelineId, String nodeId, List<String> pathId) {
        return NestMaps.NAMESPACE_PREFIX + pipelineId + "." + nodeId + "."
                + (pathId.isEmpty() ? ROOT_NAMESPACE : render(pathId));
    }

    /**
     * How a path is written wherever one is shown or named. Shared rather than rewritten per caller: a
     * vertex named one way and reported another reads as two different levels to whoever is looking.
     */
    static String render(List<String> pathId) {
        return pathId.isEmpty() ? ROOT_NAMESPACE : String.join(PATH_SEPARATOR, pathId);
    }

    private static List<String> segments(String path) {
        return List.of(path.split("\\" + PATH_SEPARATOR, -1));
    }

    private static List<Embed> childrenOf(List<Embed> embeds) {
        return embeds == null ? List.of() : embeds;
    }

    private static Node node(Embed embed, List<String> parentPathId, String parentAlias,
            List<String> parentKey) {
        List<String> pathId = new ArrayList<>(parentPathId);
        pathId.add(embed.path());
        List<Node> children = new ArrayList<>();
        for (Embed child : childrenOf(embed.embed())) {
            children.add(node(child, pathId, embed.from(), embed.key()));
        }
        return new Node(embed, List.copyOf(pathId), parentPathId, parentAlias, parentKey,
                List.copyOf(children));
    }

    /** Every node of the tree, deepest first, so a vertex is built after everything that cascades into it. */
    private static List<Node> flatten(List<Node> nodes) {
        List<Node> flat = new ArrayList<>();
        for (Node node : nodes) {
            flat.addAll(flatten(node.children()));
            flat.add(node);
        }
        return flat;
    }

    /** One embed placed in the tree: where it sits, what hangs off it, and what compiling settled for it. */
    private static final class Node {

        private final Embed embed;
        private final List<String> pathId;
        private final List<String> parentPathId;
        // The level this embed hangs under, named the way the compiler can ask about its key: the root is
        // an alias and a declared key just as an enclosing embed is, so the deep case needs no second path.
        private final String parentAlias;
        private final List<String> parentKey;
        private final List<Node> children;
        private List<String> identity = List.of();
        private List<String> arrayKey;
        private boolean referenced;

        private Node(Embed embed, List<String> pathId, List<String> parentPathId, String parentAlias,
                List<String> parentKey, List<Node> children) {
            this.embed = embed;
            this.pathId = pathId;
            this.parentPathId = parentPathId;
            this.parentAlias = parentAlias;
            this.parentKey = parentKey;
            this.children = children;
        }

        private String parentAlias() {
            return parentAlias;
        }

        private List<String> parentKey() {
            return parentKey;
        }

        /** Whether the level above points at this embed's rows, rather than its rows naming that level. */
        private boolean referenced() {
            return referenced;
        }

        private void referenced(boolean settled) {
            this.referenced = settled;
        }

        private Embed embed() {
            return embed;
        }

        private List<String> pathId() {
            return pathId;
        }

        private List<String> parentPathId() {
            return parentPathId;
        }

        private List<Node> children() {
            return children;
        }

        private List<String> identity() {
            return identity;
        }

        private void identity(List<String> settled) {
            this.identity = settled;
        }

        private List<String> arrayKey() {
            return arrayKey;
        }

        private void arrayKey(List<String> settled) {
            this.arrayKey = settled;
        }
    }
}
