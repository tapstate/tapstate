package io.tapstate.runtime.engine.nest;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.NestInspection;
import io.tapstate.runtime.engine.OperatorMapBindings;
import io.tapstate.runtime.engine.OperatorMapCoordinates;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Reads what a running nest step holds for one root key, for a person asking rather than for the run: the
 * stored assembly rendered as a {@link NestInspection}, never the stored bytes.
 *
 * <p><b>The key arrives as text and is stored typed.</b> A root key of {@code 17} may have been filed as a
 * long, an int or a decimal depending on the source, and a key of one type never finds an entry filed under
 * another. So every reading the text admits is tried, in a fixed order, and the first that holds anything
 * answers. A key typed as anything other than a number or text - a date, a binary - cannot be named from the
 * command line and is not found.
 *
 * <p>A read here goes through the map, and so through the store behind it for a key that is not in memory;
 * it changes nothing it reads. The rows the document points at are read the same way, so the document shown
 * is the one the run would render now.
 */
public final class NestInspector {

    /** More root key columns than this and the readings to try are too many to be worth trying. */
    static final int MOST_KEY_COLUMNS = 4;

    private NestInspector() {
    }

    /** What happened when a root key was looked up. */
    public sealed interface Outcome {
        /** The step holds state under the key, read out. */
        record Held(NestInspection inspection) implements Outcome { }

        /** The number of values given is not the number of columns the root key is made of. */
        record WrongArity(List<String> columns) implements Outcome { }

        /** Nothing is held under any reading of the key. */
        record NotHeld(List<String> columns) implements Outcome { }

        /** This member holds no live state map for the step. */
        record NotLive() implements Outcome { }
    }

    /**
     * Looks up {@code keyText} - one value per root key column - in the state {@code topology} keeps for
     * {@code step} of {@code pipelineId} on {@code member}.
     */
    public static Outcome inspect(HazelcastInstance member, String pipelineId, String step, NestTopology topology,
            List<String> keyText) {
        Objects.requireNonNull(member, "member");
        Objects.requireNonNull(topology, "topology");
        Objects.requireNonNull(keyText, "keyText");
        if (topology.isPassthrough()) {
            return new Outcome.NotLive();
        }
        NestVertex assembler = topology.assembler();
        List<String> columns = assembler.partitionKey();
        if (keyText.size() != columns.size()) {
            return new Outcome.WrongArity(columns);
        }
        OperatorMapBindings bindings = member.getUserContext() == null ? null
                : OperatorMapBindings.resolve(member, false);
        Optional<String> stateMap = mapFor(bindings, pipelineId, step, OperatorMapCoordinates.NestMap.STATE,
                assembler.mapName());
        if (stateMap.isEmpty()) {
            return new Outcome.NotLive();
        }
        IMap<Object, RootAssembly> assemblies = member.getMap(stateMap.get());
        for (List<Object> candidate : readings(keyText)) {
            Object key = keyOf(columns, candidate, assembler.mapName());
            RootAssembly assembly = assemblies.get(key);
            if (assembly != null) {
                boolean inTransit = mapFor(bindings, pipelineId, step, OperatorMapCoordinates.NestMap.PARKING,
                        assembler.parkingMapName())
                        .map(parking -> member.getMap(parking).containsKey(ParkedSubtree.At.ofRoot(candidate)))
                        .orElse(false);
                return new Outcome.Held(view(member, bindings, pipelineId, step, topology, columns, candidate,
                        assembly, inTransit));
            }
        }
        return new Outcome.NotHeld(columns);
    }

    private static Optional<String> mapFor(OperatorMapBindings bindings, String pipelineId, String step,
            OperatorMapCoordinates.NestMap kind, String legacy) {
        return mapFor(bindings, pipelineId, step, List.of(), kind, null, legacy);
    }

    /**
     * The name the run keeps one map under: the per-run name the member installed where the run has them,
     * otherwise the name the definition gives it.
     */
    private static Optional<String> mapFor(OperatorMapBindings bindings, String pipelineId, String step,
            List<String> path, OperatorMapCoordinates.NestMap kind, String alias, String legacy) {
        return bindings == null ? Optional.of(legacy) : bindings.liveNestMap(pipelineId, step, path, kind, alias);
    }

    /**
     * The key exactly as the run builds it: through the same path the run takes from a row, so that it is
     * the same value - a map compares keys by what they serialize to, and a list built another way is
     * another key.
     */
    private static Object keyOf(List<String> columns, List<Object> values, String namespace) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), values.get(i));
        }
        return NestKeys.valuesOf(row, columns, namespace);
    }

    /** Every typed reading of the key's text, one value per column, most likely first. */
    static List<List<Object>> readings(List<String> keyText) {
        if (keyText.size() > MOST_KEY_COLUMNS) {
            List<Object> asText = new ArrayList<>(keyText);
            return List.of(asText);
        }
        List<List<Object>> readings = new ArrayList<>();
        readings.add(new ArrayList<>());
        for (String text : keyText) {
            List<List<Object>> grown = new ArrayList<>();
            for (List<Object> prefix : readings) {
                for (Object value : valuesOf(text)) {
                    List<Object> next = new ArrayList<>(prefix);
                    next.add(value);
                    grown.add(next);
                }
            }
            readings = grown;
        }
        return readings;
    }

    private static List<Object> valuesOf(String text) {
        List<Object> values = new ArrayList<>();
        try {
            long whole = Long.parseLong(text.trim());
            values.add(whole);
            if (whole >= Integer.MIN_VALUE && whole <= Integer.MAX_VALUE) {
                values.add((int) whole);
            }
        } catch (NumberFormatException notWhole) {
            // Not a whole number; a decimal or text reading may still fit.
        }
        try {
            values.add(new BigDecimal(text.trim()));
        } catch (NumberFormatException notDecimal) {
            // Not a number at all; only the text reading is left.
        }
        values.add(text);
        return values;
    }

    private static NestInspection view(HazelcastInstance member, OperatorMapBindings bindings,
            String pipelineId, String step,
            NestTopology topology, List<String> columns, List<Object> key, RootAssembly assembly,
            boolean inTransit) {
        RootAssembly.Inspected inspected = assembly.inspect();
        List<NestInspection.Part> parts = new ArrayList<>();
        for (RootAssembly.Inspected.Part part : inspected.parts()) {
            parts.add(new NestInspection.Part(part.path(), part.key(), order(part.order()), part.deleted()));
        }
        List<NestInspection.Waiting> waiting = new ArrayList<>();
        for (RootAssembly.Inspected.Waiting held : inspected.waiting()) {
            waiting.add(new NestInspection.Waiting(held.path(), held.parentKey(), held.held()));
        }
        List<NestInspection.Missing> missing = new ArrayList<>();
        for (RootAssembly.Inspected.Missing reference : inspected.missing()) {
            missing.add(new NestInspection.Missing(reference.path(), reference.key()));
        }
        Map<String, Object> document = assembly.render(topology.slots(),
                referenced(member, bindings, pipelineId, step, topology, assembly))
                .orElse(null);
        return new NestInspection(NestInspection.VERSION, pipelineId, step, columns, key,
                inspected.rootPresent(), order(inspected.rootOrder()), parts, waiting, missing,
                inspected.unsent(), inTransit, document);
    }

    /**
     * The rows the document points at, read from the maps the run files them in. A reference whose map this
     * member does not hold live is left unresolved, and renders as the run renders a row not yet fetched.
     */
    private static Map<String, Map<Object, Map<String, Object>>> referenced(HazelcastInstance member,
            OperatorMapBindings bindings, String pipelineId, String step, NestTopology topology,
            RootAssembly assembly) {
        Map<String, NestLookup> lookups = new LinkedHashMap<>();
        topology.lookups().forEach(lookup -> lookups.put(lookup.mapName(), lookup));
        Map<String, Map<Object, Map<String, Object>>> resolved = new LinkedHashMap<>();
        assembly.referencesNeeded(topology.slots()).forEach((namespace, keys) -> {
            NestLookup lookup = lookups.get(namespace);
            Optional<String> map = lookup == null ? Optional.empty()
                    : mapFor(bindings, pipelineId, step, lookup.pathId(), OperatorMapCoordinates.NestMap.LOOKUP,
                            lookup.alias(), namespace);
            map.ifPresent(name -> {
                IMap<Object, Map<String, Object>> rows = member.getMap(name);
                // One key at a time: this is a person reading one document, and a key-set read is kept to
                // the store that resolves references while documents are assembled.
                Map<Object, Map<String, Object>> found = new LinkedHashMap<>();
                for (Object key : new LinkedHashSet<>(keys)) {
                    Map<String, Object> row = rows.get(key);
                    if (row != null) {
                        found.put(key, row);
                    }
                }
                resolved.put(namespace, found);
            });
        });
        return resolved;
    }

    private static String order(SourceOrder order) {
        return order == null ? null : order.epoch() + ":" + order.seq();
    }
}
