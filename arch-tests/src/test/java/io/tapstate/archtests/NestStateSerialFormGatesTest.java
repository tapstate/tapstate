package io.tapstate.archtests;

import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestStore;
import io.tapstate.runtime.engine.nest.ParkedSubtree;
import io.tapstate.runtime.engine.nest.ResolverState;
import io.tapstate.runtime.engine.nest.RootAssembly;
import java.io.IOException;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a nest vertex keeps outlives the process that wrote it: it is written as bytes to the layer under
 * the state map and read back by whichever build starts next. That build is routinely not the one that
 * wrote it, which makes the shape of these types a compatibility surface rather than an internal detail.
 *
 * <p>Two different failures live here, and they need two different guards.
 *
 * <p><b>A plain class that does not pin its serial identity changes identity whenever its fields change.</b>
 * The identity is derived from the shape when none is declared, so adding one field to one class makes every
 * entry already written unreadable - the read fails outright, on the first key that has to come back from
 * the cold layer, long after the upgrade looked successful. Pinning it is what stops the identity from
 * moving on its own.
 *
 * <p><b>Pinning it, on its own, trades a loud failure for a quiet one.</b> Once the identity holds still,
 * bytes written before a field existed still load - and the field that was not in them is left at its
 * zero: null, 0, false. Nothing is thrown and nothing is logged; a boolean that decides whether a document
 * is whole simply reads false. Records already behave this way and always did: their identity does not
 * move when their components change, so they have never had the loud failure to lose. That is why the
 * shape itself is written down here and compared - it is the only guard that covers the records, and the
 * only one that makes a change to any of these types something a person had to look at.
 *
 * <p>So the rule is not "the golden must never change". It is that changing it is a decision: either the
 * absent value is safe for every field added, or the identity is bumped so the old bytes are refused
 * instead of quietly misread. Both are fine; neither happens by accident.
 *
 * <p>The graph is walked rather than listed, so a type added inside one of these later is covered without
 * anyone remembering to add it. The walk stops at {@code Object}-typed fields, which hold business keys
 * and row values - JDK types and user data, each carrying its own identity, none of them ours to pin.
 */
class NestStateSerialFormGatesTest {

    /** The value roots of every state store, plus the address used to store parked subtrees. */
    // Lookup rows and reverse references currently hold only JDK containers and business values. Keep
    // their roots here so replacing either with a state class cannot leave that class outside the gate.
    private static final List<Class<?>> WHAT_IS_STORED =
            List.of(RootAssembly.class, ResolverState.class, ParkedSubtree.class, ParkedSubtree.At.class,
                    Map.class, Set.class);

    /** Ours to pin; anything outside it is a JDK type or user data that carries its own identity. */
    private static final String OURS = "io.tapstate.";

    private static final Path GOLDEN = Path.of("src", "test", "resources", "nest-state-shape.golden");

    @Test
    @DisplayName("every nest store declares a root covered by the state shape gate")
    void everyNestStoreHasARootInTheShapeGate() {
        var stores = Arrays.stream(NestBinding.NestStores.class.getDeclaredMethods())
                .filter(method -> method.getReturnType() == NestStore.class)
                .toList();
        assertThat(stores).as("the store binding must expose persisted state to this check").isNotEmpty();
        List<String> uncovered = stores.stream()
                .filter(method -> {
                    Type value = ((ParameterizedType) method.getGenericReturnType()).getActualTypeArguments()[0];
                    Type root = value instanceof ParameterizedType parameterized ? parameterized.getRawType() : value;
                    return !WHAT_IS_STORED.contains(root);
                })
                .map(method -> method.getName() + " -> "
                        + ((ParameterizedType) method.getGenericReturnType()).getActualTypeArguments()[0].getTypeName())
                .sorted()
                .toList();
        assertThat(uncovered)
                .as("every state namespace needs a root even when its value is currently a JDK container; "
                        + "otherwise replacing that container with a state class can leave its shape unpinned")
                .isEmpty();
    }

    @Test
    @DisplayName("positive control: a type that has not pinned its serial identity is still detected")
    void aTypeWithoutAPinnedIdentityIsDetected() {
        assertThat(pinsItsSerialIdentity(UnpinnedState.class))
                .as("the check no longer notices a class that declares no serialVersionUID, so it would "
                        + "pass over every stored type whether or not they declare one")
                .isFalse();
        assertThat(pinsItsSerialIdentity(PinnedState.class))
                .as("the check no longer recognises a declared serialVersionUID, so it would fail every "
                        + "stored type and have to be turned off")
                .isTrue();
    }

    @Test
    @DisplayName("the walk reaches a type named only inside a field's type arguments")
    void theWalkReachesWhatIsNamedOnlyInsideATypeArgument() {
        List<String> reached = whatTheStoredBytesReach().stream().map(Class::getName).toList();
        assertThat(reached).contains(RootAssembly.class.getName(), ResolverState.class.getName());
        assertThat(reached)
                .as("the element nodes of a document are named nowhere but inside the type arguments of "
                        + "the maps holding them, so a walk that reads only a field's own type misses "
                        + "them and every type reached through them - silently, by covering less")
                .contains(RootAssembly.class.getName() + "$ElementNode");
    }

    @Test
    @DisplayName("every plain class the stored bytes reach pins its serial identity")
    void everyStoredPlainClassPinsItsSerialIdentity() {
        List<String> unpinned = whatTheStoredBytesReach().stream()
                .filter(type -> !type.isRecord())
                .filter(type -> !pinsItsSerialIdentity(type))
                .map(Class::getName)
                .sorted()
                .toList();
        assertThat(unpinned)
                .as("these are written to the cold layer and read back by the next build: with no "
                        + "serialVersionUID declared their identity is derived from their fields, so the "
                        + "next change to any of them makes every entry already stored unreadable")
                .isEmpty();
    }

    @Test
    @DisplayName("the shape of what is stored matches the checked-in golden")
    void theStoredShapeMatchesTheGolden() throws IOException {
        List<String> golden = Files.readAllLines(GOLDEN).stream()
                .map(String::strip)
                .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                .toList();
        assertThat(storedShape())
                .as("the shape of the stored state changed. Bytes already written do not have the added "
                        + "field and will read it as null, 0 or false without saying so. Decide which "
                        + "this is and record it in the same change set: the absent value is safe for "
                        + "every field added, or the serialVersionUID is bumped so old bytes are refused "
                        + "outright instead of quietly misread")
                .isEqualTo(golden);
    }

    /**
     * Every type of ours the stored bytes can contain, found by walking what each one declares rather than
     * by listing them. Static and transient fields are left out: neither is written.
     */
    private static Set<Class<?>> whatTheStoredBytesReach() {
        Set<Class<?>> reached = new LinkedHashSet<>();
        Deque<Class<?>> toVisit = new ArrayDeque<>();
        WHAT_IS_STORED.forEach(root -> collect(root, toVisit));
        while (!toVisit.isEmpty()) {
            Class<?> type = toVisit.removeFirst();
            if (!reached.add(type)) {
                continue;
            }
            for (Field field : type.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())) {
                    continue;
                }
                collect(field.getGenericType(), toVisit);
            }
        }
        return reached;
    }

    /** Adds every one of our serializable types named anywhere inside {@code type}, generics included. */
    private static void collect(Type type, Deque<Class<?>> toVisit) {
        if (type instanceof Class<?> found) {
            if (found.getName().startsWith(OURS) && Serializable.class.isAssignableFrom(found)) {
                toVisit.addLast(found);
            }
            return;
        }
        if (type instanceof ParameterizedType parameterized) {
            collect(parameterized.getRawType(), toVisit);
            Arrays.stream(parameterized.getActualTypeArguments()).forEach(argument -> collect(argument, toVisit));
            return;
        }
        if (type instanceof GenericArrayType array) {
            collect(array.getGenericComponentType(), toVisit);
            return;
        }
        if (type instanceof WildcardType wildcard) {
            Arrays.stream(wildcard.getUpperBounds()).forEach(bound -> collect(bound, toVisit));
            Arrays.stream(wildcard.getLowerBounds()).forEach(bound -> collect(bound, toVisit));
            return;
        }
        if (type instanceof TypeVariable<?> variable) {
            Arrays.stream(variable.getBounds()).forEach(bound -> collect(bound, toVisit));
        }
    }

    /**
     * Whether {@code type} says what its serial identity is instead of having one derived from its fields.
     * A record is not asked: its identity does not move when its components change.
     */
    private static boolean pinsItsSerialIdentity(Class<?> type) {
        try {
            Field declared = type.getDeclaredField("serialVersionUID");
            return Modifier.isStatic(declared.getModifiers())
                    && Modifier.isFinal(declared.getModifiers())
                    && declared.getType() == long.class;
        } catch (NoSuchFieldException absent) {
            return false;
        }
    }

    /**
     * What is stored, as lines: one naming each type and the identity it is read back under, one per field
     * it writes. Sorted, so the file reads the same however the classes were reached.
     */
    private static List<String> storedShape() {
        List<String> lines = new ArrayList<>();
        for (Class<?> type : whatTheStoredBytesReach()) {
            lines.add(type.getName() + " :: " + (type.isRecord() ? "record" : "class")
                    + " :: uid=" + ObjectStreamClass.lookup(type).getSerialVersionUID());
            for (Field field : type.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())) {
                    continue;
                }
                lines.add(type.getName() + " :: field :: " + field.getName()
                        + " :: " + field.getGenericType().getTypeName());
            }
        }
        lines.sort(Comparator.naturalOrder());
        return List.copyOf(lines);
    }

    /** Declares no serial identity, so the check above is run against a type that really lacks one. */
    @SuppressWarnings("unused")
    private static final class UnpinnedState implements Serializable {
        private int held;
    }

    /** Declares one, so the check is also run against a type that really has one. */
    @SuppressWarnings("unused")
    private static final class PinnedState implements Serializable {
        private static final long serialVersionUID = 1L;
        private int held;
    }
}
