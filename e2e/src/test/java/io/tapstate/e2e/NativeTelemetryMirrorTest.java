package io.tapstate.e2e;

import com.sun.jdi.ArrayReference;
import com.sun.jdi.Field;
import com.sun.jdi.IntegerValue;
import com.sun.jdi.LongValue;
import com.sun.jdi.ObjectReference;
import com.sun.jdi.ReferenceType;
import com.sun.jdi.StringReference;
import com.sun.jdi.Value;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Controlled JDI field responses exercise the real complete-map decoder without a target VM. */
class NativeTelemetryMirrorTest {
    @Test
    void aChangedMapIsReadAgainAsOneCompleteStableSnapshot() throws Exception {
        MapFixture fixture = new MapFixture(false);
        var mirror = mirror();
        AtomicReference<Map<String, Object>> complete = new AtomicReference<>();
        assertThatCode(() -> complete.set(mirror.strings(fixture.wrapped()))).doesNotThrowAnyException();
        assertThat(complete.get()).containsExactlyInAnyOrderEntriesOf(Map.of("current-a", 11L, "current-b", 22L));
        assertThat(fixture.versionReads.get()).as("one rejected and one complete traversal").isEqualTo(4);
        assertThat(fixture.tableReads.get()).isEqualTo(4);
    }

    @Test
    void aMapThatKeepsChangingRemainsUnavailableAfterThreeCompleteAttempts() {
        MapFixture fixture = new MapFixture(true);
        var mirror = mirror();
        assertThatThrownBy(() -> mirror.strings(fixture.wrapped()))
                .isInstanceOf(NativeTelemetryMirror.Unavailable.class).hasMessage("INCOHERENT_MAP");
        assertThat(fixture.versionReads.get()).as("a wrapper does not multiply the three attempts").isEqualTo(6);
        assertThat(fixture.tableReads.get()).isEqualTo(6);
    }

    @Test
    void anIncoherentAttemptStillSpendsTheOriginalSharedReadBudget() throws Exception {
        MapFixture fixture = new MapFixture(false);
        var mirror = mirror();
        // Twelve reads remain. The first complete but changing traversal spends nine; a retry
        // cannot decode the two-node replacement by starting a fresh mirror or resetting its budget.
        for (int read = 0; read < 8180; read++) { mirror.field(fixture.map, "size", "I"); }
        assertThatThrownBy(() -> mirror.map(fixture.map))
                .isInstanceOf(NativeTelemetryMirror.Unavailable.class).hasMessage("MIRROR_READ_BUDGET");
        assertThat(fixture.fieldReads.get()).isEqualTo(8192);
        assertThat(fixture.versionReads.get()).as("the second attempt began under the already spent budget").isEqualTo(3);
    }

    private static NativeTelemetryMirror mirror() {
        return new NativeTelemetryMirror(type -> {
            throw new AssertionError("the controlled JDK map has no application class to validate");
        }, new HashSet<>());
    }

    private static final class MapFixture {
        final AtomicInteger versionReads = new AtomicInteger(), tableReads = new AtomicInteger(), fieldReads = new AtomicInteger();
        final ObjectReference map;
        private final boolean perpetual;
        private final ArrayReference firstTable, currentTable;
        private boolean changed;

        MapFixture(boolean perpetual) {
            this.perpetual = perpetual;
            firstTable = array(100, List.of(node(10, "retired", 1)));
            currentTable = array(101, List.of(node(11, "current-a", 11), node(12, "current-b", 22)));
            ReferenceType type = type("java.util.HashMap", Map.of("size", "I", "modCount", "I", "table", "[Ljava/util/HashMap$Node;"));
            map = proxy(ObjectReference.class, "map", (method, args) -> switch (method) {
                case "referenceType", "type" -> type;
                case "uniqueID" -> 1L;
                case "getValue" -> {
                    fieldReads.incrementAndGet();
                    yield switch (((Field) args[0]).name()) {
                        case "size" -> integer(changed ? 2 : 1);
                        case "modCount" -> {
                            int read = versionReads.incrementAndGet();
                            yield integer(perpetual ? read : changed ? 2 : 1);
                        }
                        case "table" -> {
                            int read = tableReads.incrementAndGet();
                            if (!perpetual && read == 2) { changed = true; }
                            yield changed ? currentTable : firstTable;
                        }
                        default -> throw unsupported(method);
                    };
                }
                default -> throw unsupported(method);
            });
        }

        ObjectReference wrapped() {
            ReferenceType type = type("java.util.Collections$UnmodifiableMap", Map.of("m", "Ljava/util/Map;"));
            return proxy(ObjectReference.class, "wrapper", (method, args) -> switch (method) {
                case "referenceType", "type" -> type;
                case "uniqueID" -> 2L;
                case "getValue" -> {
                    fieldReads.incrementAndGet();
                    assertThat(((Field) args[0]).name()).isEqualTo("m"); yield map;
                }
                default -> throw unsupported(method);
            });
        }

        private ObjectReference node(long id, String key, long value) {
            ReferenceType type = type("java.util.HashMap$Node", Map.of("key", "Ljava/lang/Object;", "value", "Ljava/lang/Object;",
                    "next", "Ljava/util/HashMap$Node;"));
            return proxy(ObjectReference.class, "node-" + id, (method, args) -> switch (method) {
                case "referenceType", "type" -> type;
                case "uniqueID" -> id;
                case "getValue" -> {
                    fieldReads.incrementAndGet();
                    yield switch (((Field) args[0]).name()) {
                        case "key" -> string(key);
                        case "value" -> number(value);
                        case "next" -> null;
                        default -> throw unsupported(method);
                    };
                }
                default -> throw unsupported(method);
            });
        }
    }

    private static ReferenceType type(String name, Map<String, String> fields) {
        Map<String, Field> layout = new java.util.LinkedHashMap<>();
        fields.forEach((key, signature) -> layout.put(key, proxy(Field.class, "field-" + key, (method, args) -> switch (method) {
            case "name" -> key;
            case "signature" -> signature;
            case "isStatic" -> false;
            default -> throw unsupported(method);
        })));
        return proxy(ReferenceType.class, name, (method, args) -> switch (method) {
            case "name" -> name;
            case "fieldByName" -> layout.get((String) args[0]);
            default -> throw unsupported(method);
        });
    }

    private static ArrayReference array(long id, List<? extends Value> values) {
        return proxy(ArrayReference.class, "table-" + id, (method, args) -> switch (method) {
            case "uniqueID" -> id;
            case "length" -> values.size();
            case "getValues" -> values;
            default -> throw unsupported(method);
        });
    }

    private static IntegerValue integer(int value) {
        return proxy(IntegerValue.class, "int-" + value, (method, args) -> {
            if (method.equals("value")) { return value; }
            throw unsupported(method);
        });
    }

    private static LongValue number(long value) {
        return proxy(LongValue.class, "long-" + value, (method, args) -> {
            if (method.equals("value")) { return value; }
            throw unsupported(method);
        });
    }

    private static StringReference string(String value) {
        return proxy(StringReference.class, "string-" + value, (method, args) -> {
            if (method.equals("value")) { return value; }
            throw unsupported(method);
        });
    }

    private static <T> T proxy(Class<T> contract, String label, BiFunction<String, Object[], Object> fields) {
        return contract.cast(Proxy.newProxyInstance(contract.getClassLoader(), new Class<?>[] { contract }, (self, method, args) -> {
            return switch (method.getName()) {
                case "toString" -> label;
                case "hashCode" -> System.identityHashCode(self);
                case "equals" -> self == args[0];
                default -> fields.apply(method.getName(), args == null ? new Object[0] : args);
            };
        }));
    }

    private static AssertionError unsupported(String method) {
        return new AssertionError("unexpected controlled JDI operation: " + method);
    }
}
