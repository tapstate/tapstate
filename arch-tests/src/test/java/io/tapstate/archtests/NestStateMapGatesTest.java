package io.tapstate.archtests;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.map.IMap;
import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two things a nest vertex may never do to the map holding its state, both of which work in front of you
 * and are wrong behind you.
 *
 * <p><b>Give any of it a lifetime.</b> State here is not a cache: an entry is what a later event is
 * answered from. A mapping that expires stops answering for the children that point at it, and they wait
 * for a parent that never comes back; an assembly that expires is rebuilt from whatever arrives next and
 * emitted as though it were whole. Both read as an idle pipeline, not as a failure.
 *
 * <p><b>Put the state across the map, or let an index be defined on it.</b> A write that carries the
 * state to its own key costs no serialization of the document; putting it costs one on every write, and
 * the document is the whole assembled thing. The carrying only stays free while no index exists on the
 * map - with one, what the carried write is handed is a clone of the state rather than the state - so an
 * index added years later would put the copy back with nothing to show for it and nothing reporting it.
 *
 * <p><b>Reach past one key.</b> Every edge into a nest vertex is partitioned by the key that vertex files
 * its state under, so the entry an event needs is on the member and the partition the event is already on.
 * A call that spans the map - listing it, sizing it, running over its entries - abandons that: it becomes
 * a cluster-wide operation whose cost grows with the whole keyspace rather than with the event. On a single
 * member it is merely a scan of everything and looks like it works, so the day this is wrong is the day a
 * second member appears, long after the code was written.
 *
 * <p>Both bans are checked against a fixture that really uses each mechanism before production is scanned.
 * A ban that matches nothing passes for free and keeps passing after the API it names is renamed, so the
 * positive control per mechanism is what stops this gate from quietly disarming itself.
 *
 * <p>One class is allowed to name expiry: the one that declares what a state map is, and what it declares
 * is no expiry at all. Its values are pinned by their own cases; naming it here keeps the ban from being
 * loosened for everyone else in order to let that one declaration through.
 */
class NestStateMapGatesTest {

    /** Where nest state lives. Anything reaching a state map does so from inside this package. */
    private static final String NEST = "io.tapstate.runtime.engine.nest.";

    /** The single class that may name expiry, because naming it is how it is turned off. */
    private static final String DECLARES_WHAT_A_STATE_MAP_IS = NEST + "NestMaps";

    private static final Map<String, Predicate<JavaAccess<?>>> EXPIRY_MECHANISMS = expiryMechanisms();

    private static final Map<String, Predicate<JavaAccess<?>>> WHOLE_MAP_MECHANISMS = wholeMapMechanisms();

    private static final Map<String, Predicate<JavaAccess<?>>> PUT_ACROSS_MECHANISMS = putAcrossMechanisms();

    private static final Map<String, Predicate<JavaAccess<?>>> INDEX_MECHANISMS = indexMechanisms();

    private static JavaClasses productionClasses;
    private static JavaClasses fixtureClasses;

    @BeforeAll
    static void importClasses() {
        productionClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate");
        fixtureClasses = new ClassFileImporter().importClasses(AVertexThatOversteps.class);
    }

    @Test
    @DisplayName("positive control: every banned mechanism is still detected on code that uses it")
    void eachMechanismIsDetectedOnAFixtureThatUsesIt() {
        EXPIRY_MECHANISMS.forEach((mechanism, detects) -> assertThat(accesses(fixtureClasses, detects, ""))
                .as("%s is no longer detected - the API it names has moved or been renamed, and this "
                        + "gate would pass over production code that uses it", mechanism)
                .isNotEmpty());
        WHOLE_MAP_MECHANISMS.forEach((mechanism, detects) -> assertThat(accesses(fixtureClasses, detects, ""))
                .as("%s is no longer detected - the API it names has moved or been renamed, and this "
                        + "gate would pass over production code that uses it", mechanism)
                .isNotEmpty());
        PUT_ACROSS_MECHANISMS.forEach((mechanism, detects) -> assertThat(accesses(fixtureClasses, detects, ""))
                .as("%s is no longer detected - the API it names has moved or been renamed, and this "
                        + "gate would pass over production code that uses it", mechanism)
                .isNotEmpty());
        INDEX_MECHANISMS.forEach((mechanism, detects) -> assertThat(accesses(fixtureClasses, detects, ""))
                .as("%s is no longer detected - the API it names has moved or been renamed, and this "
                        + "gate would pass over production code that uses it", mechanism)
                .isNotEmpty());
    }

    @Test
    @DisplayName("nothing a nest vertex holds is given a lifetime")
    void noNestCodeGivesStateALifetime() {
        EXPIRY_MECHANISMS.forEach((mechanism, detects) -> assertThat(accesses(productionClasses, detects, NEST))
                .as("%s: state that expires stops answering the events it exists to answer, and an "
                        + "assembly rebuilt from what arrives after it is emitted as though it were whole",
                        mechanism)
                .isEmpty());
    }

    @Test
    @DisplayName("no nest vertex reaches past the one key its event is partitioned to")
    void noNestCodeSpansTheWholeStateMap() {
        WHOLE_MAP_MECHANISMS.forEach((mechanism, detects) -> assertThat(accesses(productionClasses, detects, NEST))
                .as("%s: the entry an event needs is already on this partition, and spanning the map "
                        + "costs the whole keyspace instead - which a single member hides entirely",
                        mechanism)
                .isEmpty());
    }

    @Test
    @DisplayName("state is carried to its key rather than put across the map")
    void noNestCodePutsStateAcrossTheMap() {
        PUT_ACROSS_MECHANISMS.forEach((mechanism, detects) ->
                assertThat(accesses(productionClasses, detects, NEST))
                        .as("%s: a put serializes the whole assembled document on every write, where "
                                + "carrying it to its own key serializes none of it", mechanism)
                        .isEmpty());
    }

    @Test
    @DisplayName("no index is ever defined on a map holding nest state")
    void noNestCodeDefinesAnIndexOnAStateMap() {
        INDEX_MECHANISMS.forEach((mechanism, detects) ->
                assertThat(accesses(productionClasses, detects, NEST))
                        .as("%s: with an index defined, a carried write is handed a clone of the state "
                                + "instead of the state, and the copy this avoids comes back unreported",
                                mechanism)
                        .isEmpty());
    }

    private static Map<String, Predicate<JavaAccess<?>>> putAcrossMechanisms() {
        Map<String, Predicate<JavaAccess<?>>> mechanisms = new LinkedHashMap<>();
        named(mechanisms, "putting the state across the map", "put", "set", "putAsync", "setAsync",
                "putIfAbsent", "putTransient", "replace");
        return Map.copyOf(mechanisms);
    }

    private static Map<String, Predicate<JavaAccess<?>>> indexMechanisms() {
        Map<String, Predicate<JavaAccess<?>>> mechanisms = new LinkedHashMap<>();
        mechanisms.put("indexing a map after it exists",
                access -> onMap(access) && access.getName().equals("addIndex"));
        mechanisms.put("declaring a map that carries an index",
                access -> targets(access, MapConfig.class)
                        && (access.getName().equals("addIndexConfig")
                                || access.getName().equals("setIndexConfigs")));
        return Map.copyOf(mechanisms);
    }

    private static Map<String, Predicate<JavaAccess<?>>> expiryMechanisms() {
        Map<String, Predicate<JavaAccess<?>>> mechanisms = new LinkedHashMap<>();
        mechanisms.put("a write that carries a time to live",
                access -> onMap(access) && access.getTarget().getFullName().contains(TimeUnit.class.getName()));
        mechanisms.put("setting the time to live of an entry already written",
                access -> onMap(access) && access.getName().equals("setTtl"));
        mechanisms.put("a map configured to expire what it holds",
                access -> targets(access, MapConfig.class)
                        && (access.getName().equals("setTimeToLiveSeconds")
                                || access.getName().equals("setMaxIdleSeconds")));
        return Map.copyOf(mechanisms);
    }

    private static Map<String, Predicate<JavaAccess<?>>> wholeMapMechanisms() {
        Map<String, Predicate<JavaAccess<?>>> mechanisms = new LinkedHashMap<>();
        named(mechanisms, "listing what the map holds", "keySet", "values", "entrySet", "localKeySet");
        named(mechanisms, "measuring the whole map", "size", "isEmpty");
        named(mechanisms, "writing or removing across keys", "putAll", "setAll", "removeAll",
                "clear", "evictAll");
        named(mechanisms, "reading across keys", "getAll", "loadAll");
        named(mechanisms, "running over more than one entry", "executeOnEntries", "executeOnKeys",
                "aggregate", "project", "forEach");
        return Map.copyOf(mechanisms);
    }

    private static void named(Map<String, Predicate<JavaAccess<?>>> mechanisms, String group, String... calls) {
        Set<String> names = Set.of(calls);
        for (String call : calls) {
            mechanisms.put(group + " (" + call + ")",
                    access -> onMap(access) && names.contains(access.getName())
                            && access.getName().equals(call));
        }
    }

    private static boolean onMap(JavaAccess<?> access) {
        return targets(access, IMap.class);
    }

    private static boolean targets(JavaAccess<?> access, Class<?> owner) {
        return access.getTargetOwner().getName().equals(owner.getName());
    }

    /**
     * Every access made from within {@code fromPackage} that the mechanism matches, named by where it was
     * made. The class allowed to declare what a state map is is left out: it names expiry in order to turn
     * it off, and what it sets is pinned where that declaration is read back.
     */
    private static List<String> accesses(JavaClasses classes, Predicate<JavaAccess<?>> detects,
            String fromPackage) {
        return classes.stream()
                .flatMap(type -> type.getAccessesFromSelf().stream())
                .filter(access -> access.getOriginOwner().getName().startsWith(fromPackage))
                .filter(access -> !access.getOriginOwner().getName().equals(DECLARES_WHAT_A_STATE_MAP_IS))
                .filter(detects)
                .map(access -> access.getOriginOwner().getName() + " -> " + access.getTarget().getFullName())
                .toList();
    }

    /**
     * Uses every banned mechanism, so the detectors above are checked against real bytecode rather than
     * against a name that may no longer exist. Never called: it exists to be read by the importer.
     */
    @SuppressWarnings("unused")
    private static final class AVertexThatOversteps {

        private static void writesThatExpire(IMap<Object, Object> map) {
            map.put("k", "v", 1, TimeUnit.MINUTES);
            map.set("k", "v", 1, TimeUnit.MINUTES);
            map.putIfAbsent("k", "v", 1, TimeUnit.MINUTES);
            map.putTransient("k", "v", 1, TimeUnit.MINUTES);
            map.setTtl("k", 1, TimeUnit.MINUTES);
        }

        private static MapConfig aMapThatExpires() {
            return new MapConfig("expiring").setTimeToLiveSeconds(60).setMaxIdleSeconds(60);
        }

        private static void putsStateAcrossTheMap(IMap<Object, Object> map) {
            map.put("k", "v");
            map.set("k", "v");
            map.putAsync("k", "v");
            map.setAsync("k", "v");
            map.putIfAbsent("k", "v");
            map.putTransient("k", "v", 1, TimeUnit.MINUTES);
            map.replace("k", "v");
        }

        private static void indexesTheMap(IMap<Object, Object> map) {
            map.addIndex(new IndexConfig(IndexType.HASH, "field"));
        }

        private static MapConfig aMapThatCarriesAnIndex() {
            return new MapConfig("indexed")
                    .addIndexConfig(new IndexConfig(IndexType.HASH, "field"))
                    .setIndexConfigs(List.of(new IndexConfig(IndexType.HASH, "field")));
        }

        private static void spansTheMap(IMap<Object, Object> map) {
            map.keySet();
            map.values();
            map.entrySet();
            map.localKeySet();
            map.size();
            map.isEmpty();
            map.putAll(Map.of());
            map.setAll(Map.of());
            map.removeAll(entry -> true);
            map.clear();
            map.evictAll();
            map.getAll(Set.of());
            map.loadAll(true);
            map.executeOnEntries(entry -> null);
            map.executeOnKeys(Set.of(), entry -> null);
            map.aggregate(null);
            map.project(null);
            map.forEach((key, value) -> { });
        }
    }
}
