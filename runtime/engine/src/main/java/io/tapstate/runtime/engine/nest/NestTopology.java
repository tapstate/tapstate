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
 */
public record NestTopology(List<NestVertex> vertices, List<NestStream> streams, List<EmbedSlot> slots)
        implements Serializable {

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
            return new NestTopology(List.of(), List.of(), List.of());
        }

        List<Node> top = new ArrayList<>();
        for (Embed embed : declared) {
            top.add(node(embed, List.of()));
        }
        List<Node> all = flatten(top);

        List<String> rootKey = root.key() == null ? List.of() : root.key();
        if (rootKey.isEmpty()) {
            throw new TapstateException(NestError.ROOT_KEY_REQUIRED,
                    Map.of("rootAlias", root.from()), null);
        }
        checkPaths(top);
        for (Node node : all) {
            checkPaths(node.children());
        }
        List<String> rootIdentity = identityOf(ROOT_NAMESPACE, rootKey, top);
        for (Node node : all) {
            if (!node.children().isEmpty()) {
                node.identity(identityOf(render(node.pathId()), null, node.children()));
            }
        }
        for (Node node : all) {
            node.arrayKey(resolveArrayKey(node, tables));
        }
        checkAppendMode(root, all);
        checkVertexCount(all, resolverVertexLimit);

        return assemble(pipelineId, nodeId, root, rootIdentity, top, all);
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
     */
    public Set<String> stateNamespaces() {
        Set<String> namespaces = new LinkedHashSet<>();
        for (NestVertex vertex : vertices) {
            namespaces.add(vertex.mapName());
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
            List<String> rootIdentity, List<Node> top, List<Node> all) {
        Map<List<String>, List<String>> identities = new LinkedHashMap<>();
        identities.put(List.of(), rootIdentity);
        for (Node node : all) {
            identities.put(node.pathId(), node.identity());
        }
        List<NestVertex> vertices = new ArrayList<>();
        List<NestStream> streams = new ArrayList<>();
        for (Node node : all) {
            if (!node.children().isEmpty()) {
                vertices.add(vertexOf(pipelineId, nodeId, node, identities.get(node.parentPathId())));
            }
        }
        String assemblerName = vertexName(nodeId, List.of());
        vertices.add(new NestVertex(List.of(), assemblerName,
                mapName(pipelineId, nodeId, List.of()), rootIdentity, List.of(),
                edgesOf(root.from(), List.of(), List.of(), rootIdentity, top)));

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
        return new NestTopology(vertices, streams, slotsOf(top));
    }

    private static NestVertex vertexOf(String pipelineId, String nodeId, Node node, List<String> parentIdentity) {
        return new NestVertex(node.pathId(), vertexName(nodeId, node.pathId()),
                mapName(pipelineId, nodeId, node.pathId()), node.identity(),
                joinFields(node.embed(), parentIdentity),
                edgesOf(node.embed().from(), node.pathId(), node.arrayKey(), node.identity(), node.children()));
    }

    /**
     * The edges into one vertex: its own rows first, then one per child embed. A leaf child arrives
     * directly, keyed by the join field it carries; a child with children of its own arrives cascading
     * from its own vertex, already stamped. The count is one plus the number of children - two only when
     * there happens to be exactly one child.
     */
    private static List<NestInbound> edgesOf(String ownAlias, List<String> ownPathId, List<String> ownElementKey,
            List<String> identity, List<Node> children) {
        List<NestInbound> edges = new ArrayList<>();
        edges.add(new NestInbound(0, ownAlias, ownPathId, identity, ownElementKey));
        for (Node child : children) {
            edges.add(child.children().isEmpty()
                    ? new NestInbound(edges.size(), child.embed().from(), child.pathId(),
                            joinFields(child.embed(), identity), child.arrayKey())
                    : new NestInbound(edges.size(), null, child.pathId(), List.of(), List.of()));
        }
        return edges;
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
    private static List<String> resolveArrayKey(Node node, Function<String, NestTable> tables) {
        List<String> declared = node.embed().arrayKey();
        if (declared != null && !declared.isEmpty()) {
            return declared;
        }
        String alias = node.embed().from();
        NestTable table = tables.apply(alias);
        if (table == null) {
            throw new IllegalStateException("nest alias '" + alias + "' resolves to no table");
        }
        if (table.primaryKey().isEmpty()) {
            throw new TapstateException(NestError.ARRAY_KEY_UNRESOLVABLE,
                    Map.of("embedPath", render(node.pathId()), "table", table.name()), null);
        }
        return table.primaryKey();
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

    private static Node node(Embed embed, List<String> parentPathId) {
        List<String> pathId = new ArrayList<>(parentPathId);
        pathId.add(embed.path());
        List<Node> children = new ArrayList<>();
        for (Embed child : childrenOf(embed.embed())) {
            children.add(node(child, pathId));
        }
        return new Node(embed, List.copyOf(pathId), parentPathId, List.copyOf(children));
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
        private final List<Node> children;
        private List<String> identity = List.of();
        private List<String> arrayKey;

        private Node(Embed embed, List<String> pathId, List<String> parentPathId, List<Node> children) {
            this.embed = embed;
            this.pathId = pathId;
            this.parentPathId = parentPathId;
            this.children = children;
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
