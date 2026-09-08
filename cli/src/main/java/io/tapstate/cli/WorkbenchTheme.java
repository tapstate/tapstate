package io.tapstate.cli;

import dev.tamboui.css.Styleable;
import dev.tamboui.css.engine.StyleEngine;
import dev.tamboui.style.Color;
import dev.tamboui.style.Style;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Central semantic styles for the workbench, backed by packaged TamboUI stylesheets. */
final class WorkbenchTheme {

    private static final Set<TokenName> REQUIRED = Set.of(TokenName.values());

    private final Mode mode;
    private final Map<TokenName, Style> styles;
    private final boolean fallbackUsed;

    private WorkbenchTheme(Mode mode, Map<TokenName, Style> styles, boolean fallbackUsed) {
        this.mode = Objects.requireNonNull(mode, "mode");
        this.styles = Map.copyOf(styles);
        this.fallbackUsed = fallbackUsed;
    }

    static WorkbenchTheme dark() {
        return load(Mode.DARK);
    }

    static WorkbenchTheme load(Mode mode) {
        return load(mode, WorkbenchTheme::readResource);
    }

    static WorkbenchTheme load(Mode mode, ResourceLoader loader) {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(loader, "loader");
        try {
            StyleEngine engine = StyleEngine.create();
            engine.addStylesheet(mode.id, loader.load(mode.resource));
            engine.setActiveStylesheet(mode.id);
            EnumMap<TokenName, Style> resolved = new EnumMap<>(TokenName.class);
            for (TokenName token : REQUIRED) {
                Style style = engine.resolve(new ThemeToken(token.cssId)).toStyle();
                if (style.equals(Style.EMPTY)) {
                    throw new IllegalStateException("Missing workbench theme token: " + token.cssId);
                }
                resolved.put(token, style);
            }
            return new WorkbenchTheme(mode, resolved, false);
        } catch (IOException | RuntimeException unavailable) {
            return new WorkbenchTheme(mode, fallback(mode), true);
        }
    }

    Mode mode() {
        return mode;
    }

    boolean fallbackUsed() {
        return fallbackUsed;
    }

    Style base() {
        return style(TokenName.BASE);
    }

    Style accent() {
        return style(TokenName.ACCENT);
    }

    Style accentBackground() {
        return style(TokenName.ACCENT_BACKGROUND);
    }

    Style hintKey() {
        return style(TokenName.HINT_KEY);
    }

    Style title() {
        return style(TokenName.TITLE);
    }

    Style muted() {
        return style(TokenName.MUTED);
    }

    Style selection() {
        return style(TokenName.SELECTION);
    }

    Style success() {
        return style(TokenName.SUCCESS);
    }

    Style warning() {
        return style(TokenName.WARNING);
    }

    Style error() {
        return style(TokenName.ERROR);
    }

    Style info() {
        return style(TokenName.INFO);
    }

    Style label() {
        return style(TokenName.LABEL);
    }

    private Style style(TokenName token) {
        return styles.get(token);
    }

    private static String readResource(String path) throws IOException {
        InputStream stream = WorkbenchTheme.class.getClassLoader().getResourceAsStream(path);
        if (stream == null) {
            ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
            if (contextLoader != null) {
                stream = contextLoader.getResourceAsStream(path);
            }
        }
        if (stream == null) {
            throw new IOException("Workbench theme resource not found: " + path);
        }
        try (InputStream input = stream) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static Map<TokenName, Style> fallback(Mode mode) {
        Color accent = mode.accent;
        Color accentForeground = mode == Mode.DARK ? Color.BLACK : Color.WHITE;
        EnumMap<TokenName, Style> fallback = new EnumMap<>(TokenName.class);
        if (mode == Mode.DARK) {
            fallback.put(TokenName.BASE, Style.EMPTY.fg(Color.hex("#D4D4D4")).bg(Color.hex("#1E1E1E")));
            fallback.put(TokenName.MUTED, Style.EMPTY.fg(Color.hex("#8A9494")));
            fallback.put(TokenName.SELECTION,
                    Style.EMPTY.fg(Color.WHITE).bg(Color.hex("#285A63")).bold());
            fallback.put(TokenName.INFO, Style.EMPTY.fg(Color.hex("#8CCFD8")));
            fallback.put(TokenName.LABEL, Style.EMPTY.fg(Color.hex("#D7C986")));
        } else {
            fallback.put(TokenName.BASE, Style.EMPTY.fg(Color.hex("#203033")).bg(Color.hex("#F7FAFA")));
            fallback.put(TokenName.MUTED, Style.EMPTY.fg(Color.hex("#647477")));
            fallback.put(TokenName.SELECTION,
                    Style.EMPTY.fg(Color.BLACK).bg(Color.hex("#B9E1E5")).bold());
            fallback.put(TokenName.INFO, Style.EMPTY.fg(Color.hex("#225D8A")));
            fallback.put(TokenName.LABEL, Style.EMPTY.fg(Color.hex("#725E12")));
        }
        fallback.put(TokenName.ACCENT, Style.EMPTY.fg(accent));
        fallback.put(TokenName.ACCENT_BACKGROUND, Style.EMPTY.fg(accentForeground).bg(accent).bold());
        fallback.put(TokenName.HINT_KEY, Style.EMPTY.fg(accentForeground).bg(accent).bold());
        fallback.put(TokenName.TITLE, Style.EMPTY.fg(accent).bold());
        fallback.put(TokenName.SUCCESS, Style.EMPTY.fg(Color.hex(mode == Mode.DARK ? "#4EC9B0" : "#16785B")));
        fallback.put(TokenName.WARNING, Style.EMPTY.fg(Color.hex(mode == Mode.DARK ? "#DCDCAA" : "#8A6500")));
        fallback.put(TokenName.ERROR, Style.EMPTY.fg(Color.hex(mode == Mode.DARK ? "#F48771" : "#B42318")));
        return fallback;
    }

    enum Mode {
        DARK("dark", "tui/themes/tapstate-dark.tcss", Color.hex("#4FACBC")),
        LIGHT("light", "tui/themes/tapstate-light.tcss", Color.hex("#2A7481"));

        private final String id;
        private final String resource;
        private final Color accent;

        Mode(String id, String resource, Color accent) {
            this.id = id;
            this.resource = resource;
            this.accent = accent;
        }
    }

    @FunctionalInterface
    interface ResourceLoader {
        String load(String path) throws IOException;
    }

    private enum TokenName {
        BASE("base"),
        ACCENT("accent"),
        ACCENT_BACKGROUND("accent-bg"),
        HINT_KEY("hint-key"),
        TITLE("title"),
        MUTED("muted"),
        SELECTION("selection"),
        SUCCESS("success"),
        WARNING("warning"),
        ERROR("error"),
        INFO("info"),
        LABEL("label");

        private final String cssId;

        TokenName(String cssId) {
            this.cssId = cssId;
        }
    }

    private record ThemeToken(String id) implements Styleable {
        @Override
        public Optional<String> cssId() {
            return Optional.of(id);
        }

        @Override
        public Set<String> cssClasses() {
            return Collections.emptySet();
        }

        @Override
        public Optional<Styleable> cssParent() {
            return Optional.empty();
        }
    }
}
