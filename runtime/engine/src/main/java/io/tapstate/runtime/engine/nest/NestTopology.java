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
        List<NestLookup> lookups, boolean foldingAllowed) implements Serializable {

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
        lookups = List.copyOf(lookups);
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
            return new NestTopology(List.of(), List.of(), List.of(), List.of(), true);
        }

        // The root is a level like any other: what identifies one of its rows is asked of the same four
        // places, so a root over a table that declares a key need not repeat it. What differs is the
        // ending. A root nothing identifies is the author's own missing declaration and says so; the
        // refusal an embed gets names an array key and a path, neither of which a root has - it would
        // arrive carrying the internal name of the root's own namespace in the slot meant for a path.
        List<String> rootKey = keyOrNone(root.from(), root.key(), ROOT_NAMESPACE, tables);
        if (rootKey == null) {
            throw new TapstateException(NestError.ROOT_KEY_REQUIRED,
                    Map.of("rootAlias", root.from()), null);
        }
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
                //
                // What identifies a row, and never what the author wrote to tell its elements apart inside
                // one document. The two are different keys and the difference only shows when this level
                // points at something: what is recorded about who points where is written against this
                // identity, and a number that restarts at one in every parent is the same value in every
                // document - so the record of two elements in two documents is one entry, and an edit to
                // the row they both name reaches whichever of them wrote that entry last. Every document
                // is right until the moment such a row is edited, and then one of them silently is not.
                node.identity(claiming.isEmpty()
                        ? rowKeyOf(node, tables)
                        : identityOf(render(node.pathId()), null, claiming));
            }
        }
        for (Node node : all) {
            node.arrayKey(resolveArrayKey(node, tables));
        }
        checkReferencedLevelsAreLeaves(all, tables);
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
        // The rows a level points at are state exactly as much as the documents holding them, and are the
        // one namespace here nothing else would take down: no vertex is named for it, so a tree dropped
        // without naming it would leave every row it ever pointed at behind, under a name that is now
        // reachable from nothing.
        for (NestLookup lookup : lookups) {
            namespaces.add(lookup.mapName());
            // And which rows point at each of them, which grows with the fanout where the rows themselves
            // do not. It is the one namespace here on no functional path at all - nothing reads it to
            // render a document - so it is also the one a set built from what a run touches would miss.
            namespaces.add(lookup.referencesMapName());
        }
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
        List<NestLookup> lookups = new ArrayList<>();
        for (Node node : all) {
            if (node.referenced()) {
                // A row the level points at goes to a place of its own rather than into any document: it
                // is one row shared by however many documents name it, so it enters the tree once and is
                // read from there. It takes no hops - nothing cascades from it, and the read that finds it
                // happens where the document is rendered rather than on the way in.
                NestVertex pointing = vertexAt(vertices, node.parentPathId());
                NestLookup lookup = new NestLookup(node.pathId(), node.embed().from(),
                        lookupName(nodeId, node.pathId()), mapName(pipelineId, nodeId, node.pathId()),
                        referenceIdentity(node.embed()), node.parentAlias(),
                        referenceFields(node.embed()), identities.get(node.parentPathId()),
                        node.parentPathId(), touchOrdinal(pointing, node),
                        // Read off the edge that level's own rows arrive on rather than off the tree again:
                        // it is the same switch, and asking it twice is how the two answers start to differ.
                        pointing.inbound().get(0).tracksKeyChanges());
                lookups.add(lookup);
                streams.add(new NestStream(node.embed().from(), node.pathId(), 0, lookup.name(), null));
                continue;
            }
            boolean leaf = node.children().isEmpty();
            int depth = node.pathId().size();
            String entry = leaf
                    ? vertexName(nodeId, node.parentPathId())
                    : vertexName(nodeId, node.pathId());
            streams.add(new NestStream(node.embed().from(), node.pathId(), leaf ? depth - 1 : depth,
                    entry, node.arrayKey()));
        }
        return new NestTopology(vertices, streams, slotsOf(pipelineId, nodeId, top), lookups,
                !WriteMode.APPEND.yaml().equals(root.mode()));
    }

    /**
     * Which inbound ordinal of the level doing the pointing carries word that {@code referenced} has been
     * edited. Read back off the edge the compiler already built rather than counted again here: an ordinal
     * worked out twice is an ordinal that can disagree with itself, and the disagreement would land the
     * word on some other edge's handling with nothing to say so.
     */
    private static int touchOrdinal(NestVertex pointing, Node referenced) {
        for (NestInbound edge : pointing.inbound()) {
            if (edge.carriesTouches() && edge.pathId().equals(referenced.pathId())) {
                return edge.ordinal();
            }
        }
        throw new IllegalStateException("no edge into the level above " + referenced.pathId()
                + " carries word that it was edited");
    }

    /** The compiled vertex serving {@code pathId}. Every level a reference hangs off has one. */
    private static NestVertex vertexAt(List<NestVertex> vertices, List<String> pathId) {
        for (NestVertex vertex : vertices) {
            if (vertex.pathId().equals(pathId)) {
                return vertex;
            }
        }
        throw new IllegalStateException("no vertex was built for " + pathId);
    }

    /**
     * Refuses a level the document points at that carries embeds of its own. Such a row belongs to no one
     * document - the same row sits under however many point at it - so there is no document its children
     * could be gathered under, and nothing says which of them a change beneath it should reach.
     *
     * <p>Refused rather than left to compile. The two directions are written identically, so this is an
     * ordinary thing to write by accident; and left alone it builds a level whose rows are filed where the
     * pointed-at rows themselves are kept, one namespace holding two unrelated kinds of state, while every
     * document still goes out looking complete.
     */
    private static void checkReferencedLevelsAreLeaves(List<Node> all, Function<String, NestTable> tables) {
        for (Node node : all) {
            if (!node.referenced() || node.children().isEmpty()) {
                continue;
            }
            List<String> beneath = new ArrayList<>();
            for (Node child : node.children()) {
                beneath.add(child.embed().path());
            }
            NestTable table = tables.apply(node.embed().from());
            throw new TapstateException(NestError.REFERENCED_LEVEL_CARRIES_EMBEDS,
                    Map.of("embedPath", render(node.pathId()),
                            "table", table == null ? node.embed().from() : table.name(),
                            "children", String.join(", ", beneath)), null);
        }
    }

    /**
     * The columns identifying the row an embed points at, in the order its {@code on} map wrote them. Both
     * sides of the reference build their key by walking that same order - the row being pointed at off
     * these columns, the level pointing at it off the ones they are mapped to - so neither side has to know
     * anything about the other's table for the two keys to match.
     */
    private static List<String> referenceIdentity(Embed embed) {
        return List.copyOf(embed.on().keySet());
    }

    /** The columns on the pointing level that hold the reference, in that same order. */
    private static List<String> referenceFields(Embed embed) {
        return List.copyOf(embed.on().values());
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
        // Last of all, one per level this one points at: the edge it is told on that such a row has been
        // edited. It is the pointed-at row's only way back to a document - the row belongs to no document,
        // so a change to it routes itself nowhere - and it arrives from the vertex filing those rows rather
        // than from a source, which is why it is drawn separately from every edge above.
        for (Node child : children) {
            if (child.referenced()) {
                edges.add(NestInbound.touchesOf(edges.size(), child.embed().from(), child.pathId(),
                        identity));
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
                keyOrNone(node.parentAlias(), node.parentKey(), render(node.parentPathId()), tables);
        if (parentKey == null) {
            // Nothing identifies the level above, so neither side of this join can be read as pointing at
            // it and there is no direction to infer. The tree keeps the one it has always had rather than
            // being refused for want of an answer it never needed - which is where every level a source
            // discovered nothing about lands, an embed under a level fed by an earlier step among them.
            return false;
        }
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
        List<String> key = keyOrNone(alias, declared, owner, tables);
        if (key == null) {
            throw new TapstateException(NestError.ARRAY_KEY_UNRESOLVABLE,
                    Map.of("embedPath", owner, "table", tables.apply(alias).name()), null);
        }
        return key;
    }

    /**
     * The same four places, with the fourth answered rather than refused: null where nothing identifies a
     * row here. Two callers want the two different endings. Carrying an unknown identity into the assembly
     * is not something a level may do, so asking for one is a refusal; asking which way a join points is a
     * question that simply has no answer without a key, and the tree that raised it was never relying on
     * one - refusing there would turn every level a source discovered nothing about into a broken artifact.
     *
     * <p>An ambiguous key stays a refusal on both paths. Absence is a question with no answer; two answers
     * is a choice, and nothing here is entitled to make it on the author's behalf.
     */
    private static List<String> keyOrNone(String alias, List<String> declared, String owner,
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
        return null;
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

    /**
     * How many vertices of its own this tree compiles to, against the limit. A level the document points at
     * is counted alongside the levels that resolve one: it takes a vertex, and that vertex is not
     * cooperative either, so it costs a thread by exactly the same measure. Counting only the resolvers
     * would leave a tree pointing at forty tables paying for forty threads the limit never saw.
     */
    private static void checkVertexCount(List<Node> all, int limit) {
        int vertices = 0;
        for (Node node : all) {
            if (node.referenced() || !node.children().isEmpty()) {
                vertices++;
            }
        }
        if (vertices > limit) {
            throw new TapstateException(NestError.RESOLVER_VERTEX_LIMIT_EXCEEDED,
                    Map.of("vertices", vertices, "limit", limit), null);
        }
    }

    /**
     * The shape the assembler renders documents into: which field each embed occupies and whether an
     * absent one shows as an empty array or not at all. It is a property of the declared tree and not of
     * the data, which is why it is settled here rather than remembered per document.
     */
    private static List<EmbedSlot> slotsOf(String pipelineId, String nodeId, List<Node> nodes) {
        List<EmbedSlot> slots = new ArrayList<>();
        for (Node node : nodes) {
            Embed embed = node.embed();
            slots.add(node.referenced()
                    ? new EmbedSlot(embed.path(), embed.as(), referenceFields(embed),
                            mapName(pipelineId, nodeId, node.pathId()),
                            slotsOf(pipelineId, nodeId, node.children()))
                    : new EmbedSlot(embed.path(), embed.as(), slotsOf(pipelineId, nodeId, node.children())));
        }
        return slots;
    }

    private static String vertexName(String nodeId, List<String> pathId) {
        return pathId.isEmpty() ? "nest:" + nodeId : "nest:" + nodeId + ":" + render(pathId);
    }

    /**
     * The name of the vertex that files away the rows one level points at. Suffixed rather than sharing the
     * name a resolver at that path would take: the two are different jobs over the same path, and a graph
     * refusing a duplicate name is the only thing that would say so.
     */
    private static String lookupName(String nodeId, List<String> pathId) {
        return vertexName(nodeId, pathId) + ":lookup";
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
