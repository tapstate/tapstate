package io.tapstate.cli;

import org.jline.utils.AttributedString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class TuiInteractionContractTest {

    private static final TuiDashboard DASHBOARD = new TuiDashboard();

    @ParameterizedTest(name = "{0}")
    @MethodSource("frames")
    void rendersTheFrozenFrameForEachConnectionStateAndBreakpoint(
            String name, TuiDashboard.Connection connection, int width, int height) throws IOException {
        List<String> rendered = DASHBOARD.render(state(connection), width, height).stream()
                .map(AttributedString::toString)
                .toList();
        List<String> actual = rendered.stream()
                .map(String::stripTrailing)
                .toList();

        assertThat(actual).containsExactlyElementsOf(golden(name));
        assertThat(rendered).allSatisfy(line -> assertThat(line).hasSize(width));
    }

    @Test
    void rendersNoContextOnboardingAsAnActionableLocalState() {
        List<String> frame = DASHBOARD.render(TuiDashboard.State.onboarding(Path.of("orders")), 80, 24).stream()
                .map(AttributedString::toString)
                .toList();

        assertThat(frame).anyMatch(line -> line.contains("no context"))
                .anyMatch(line -> line.contains("Create or choose context"))
                .anyMatch(line -> line.contains("Stay offline"));
    }

    @Test
    void writeConfirmationAlwaysNamesTheTargetAndIssuerWithoutRelyingOnColor() {
        TuiDashboard.Prompt prompt = TuiDashboard.Prompt.confirmWrite("pipeline.start", "production", "01J5ABCD");

        assertThat(prompt.question()).contains("pipeline.start").contains("production").contains("01J5ABCD");
        assertThat(prompt.options()).containsExactly("Cancel", "Run");
    }

    static Stream<Arguments> frames() {
        return Stream.of(
                TuiDashboard.Connection.SIGNED_OUT,
                TuiDashboard.Connection.CONNECTING,
                TuiDashboard.Connection.ONLINE,
                TuiDashboard.Connection.OFFLINE,
                TuiDashboard.Connection.SESSION_EXPIRED,
                TuiDashboard.Connection.ISSUER_MISMATCH)
                .flatMap(connection -> Stream.of(
                                Arguments.of(connection.label() + "-wide", connection, 120, 30),
                                Arguments.of(connection.label() + "-single", connection, 80, 24),
                                Arguments.of(connection.label() + "-compact", connection, 50, 12)));
    }

    private static TuiDashboard.State state(TuiDashboard.Connection connection) {
        return new TuiDashboard.State(Path.of("/work/orders"), "dev", "alice@example.com", connection,
                null, "", List.of(), 0, null, "http://127.0.0.1:8081", null, null,
                List.of("✓ resumed session"), List.of());
    }

    private static List<String> golden(String name) throws IOException {
        int suffix = name.lastIndexOf('-');
        String state = name.substring(0, suffix);
        String viewport = name.substring(suffix + 1);
        try (var stream = TuiInteractionContractTest.class.getResourceAsStream("/tui/frames/" + viewport + ".txt")) {
            if (stream == null) {
                throw new IOException("missing TUI frame golden file: " + viewport);
            }
            String content = new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).stripTrailing();
            Map<String, List<String>> frames = new LinkedHashMap<>();
            String current = null;
            for (String line : content.split("\\R", -1)) {
                if (line.startsWith("[") && line.endsWith("]")) {
                    current = line.substring(1, line.length() - 1);
                    frames.put(current, new java.util.ArrayList<>());
                } else if (line.startsWith("{blank*") && line.endsWith("}")) {
                    int count = Integer.parseInt(line.substring("{blank*".length(), line.length() - 1));
                    for (int index = 0; index < count; index++) {
                        frames.get(current).add("");
                    }
                } else if (current != null) {
                    frames.get(current).add(line);
                }
            }
            List<String> frame = frames.get(state);
            if (frame == null) {
                throw new IOException("missing TUI frame golden: " + name);
            }
            return List.copyOf(frame);
        }
    }
}
