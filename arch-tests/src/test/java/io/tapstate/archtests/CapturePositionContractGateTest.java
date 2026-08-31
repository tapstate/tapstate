package io.tapstate.archtests;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.SourcePosition;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Where a read starts, and where a snapshot hands its change tail off, are carried by four types on the
 * capture port. Every connector adapter implements them and the runtime calls them, so their shape is a
 * contract rather than an internal detail — and a second capture path is going to be built against
 * exactly this shape, which is why it is written down before that path exists rather than after.
 *
 * <p>What this guards against is not a rename. It is a signature quietly losing the part that carries a
 * position: a start parameter dropped so the port picks for a caller who never said, a seam narrowed to
 * something callers may ignore. Both compile, both leave every existing test green, and both produce a
 * pipeline that runs, reports healthy, and skips whatever changed between where it should have resumed
 * and where it actually began.
 *
 * <p>So the rule is not "this file must never change". It is that changing it is a decision somebody
 * made on purpose: a position that stops being threaded is the one failure in this area that nothing
 * else in the build notices.
 */
class CapturePositionContractGateTest {

    /**
     * The read-side contract. Everything that carries a position is reached from one of these.
     *
     * <p>{@code CaptureListener} is one of them because a change now arrives with the position the source
     * stated for it. Dropping that parameter compiles, leaves the build green, and leaves the durable read
     * offset with nothing to advance on — a pipeline that runs, reports healthy, and resumes from the
     * beginning every time.
     */
    private static final List<Class<?>> CONTRACT = List.of(
            CapturePort.class, CaptureBatch.class, CaptureStart.class, SourcePosition.class,
            CaptureListener.class);

    private static final Path GOLDEN = Path.of("src", "test", "resources", "capture-position-contract.golden");

    /** A two-argument cdc: the shape this contract had before a caller could say where to start. */
    private interface PositionlessPort {
        Object cdc(Object config, Object listener);
    }

    @Test
    @DisplayName("positive control: a signature that has lost its start parameter renders differently")
    void aDroppedStartParameterIsVisibleInTheRendering() {
        List<String> positionless = render(List.of(PositionlessPort.class));

        assertThat(positionless)
                .as("the rendering no longer carries parameter types, so a dropped parameter would compare "
                        + "equal and this golden would pass over the change it exists to catch")
                .singleElement(org.assertj.core.api.InstanceOfAssertFactories.STRING)
                .contains("cdc(java.lang.Object, java.lang.Object)");

        assertThat(render(List.of(CapturePort.class)))
                .filteredOn(line -> line.contains(" :: method :: cdc("))
                .singleElement(org.assertj.core.api.InstanceOfAssertFactories.STRING)
                .contains("io.tapstate.spi.capture.CaptureStart");
    }

    @Test
    void theReadSidePositionContractMatchesTheGolden() throws IOException {
        assertThat(render(CONTRACT))
                .as("the read-side position contract changed; if that was the intent, update %s", GOLDEN)
                .containsExactlyElementsOf(golden());
    }

    /** The recorded shape: comment and blank lines are prose, everything else is a line of contract. */
    private static List<String> golden() throws IOException {
        return Files.readAllLines(GOLDEN).stream()
                .map(String::strip)
                .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                .toList();
    }

    /**
     * Renders the given types and everything a sealed one permits: each public declared method with its
     * full parameter and return types, each record component with its type, each permitted subclass.
     * Sorted, so the file reads as a set rather than as a declaration order nobody chose.
     */
    private static List<String> render(List<Class<?>> roots) {
        Set<Class<?>> seen = new LinkedHashSet<>();
        Deque<Class<?>> pending = new ArrayDeque<>(roots);
        List<String> lines = new ArrayList<>();
        while (!pending.isEmpty()) {
            Class<?> type = pending.poll();
            if (!seen.add(type)) {
                continue;
            }
            String owner = type.getName();
            if (type.isSealed()) {
                for (Class<?> permitted : type.getPermittedSubclasses()) {
                    lines.add(owner + " :: permits :: " + permitted.getName());
                    pending.add(permitted);
                }
            }
            if (type.isRecord()) {
                // A record's methods are mostly ones the compiler wrote; its components are the shape.
                for (RecordComponent component : type.getRecordComponents()) {
                    lines.add(owner + " :: component :: " + component.getName()
                            + " :: " + component.getGenericType().getTypeName());
                }
                continue;
            }
            for (Method method : type.getDeclaredMethods()) {
                if (method.isSynthetic() || method.isBridge() || !Modifier.isPublic(method.getModifiers())) {
                    continue;
                }
                lines.add(owner + " :: method :: " + signature(method));
            }
        }
        return lines.stream().sorted().toList();
    }

    /** One method, with the types it takes and the type it answers with — erasure would hide the point. */
    private static String signature(Method method) {
        String parameters = Arrays.stream(method.getGenericParameterTypes())
                .map(Type::getTypeName)
                .collect(Collectors.joining(", "));
        return method.getName() + "(" + parameters + ") -> " + method.getGenericReturnType().getTypeName();
    }
}
