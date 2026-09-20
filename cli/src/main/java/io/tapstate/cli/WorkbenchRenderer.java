package io.tapstate.cli;

import dev.tamboui.layout.Rect;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Layout;
import dev.tamboui.style.Overflow;
import dev.tamboui.style.Style;
import dev.tamboui.terminal.Frame;
import dev.tamboui.text.CharWidth;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.block.Title;
import dev.tamboui.widgets.Clear;
import dev.tamboui.widgets.paragraph.Paragraph;
import dev.tamboui.widgets.scrollbar.Scrollbar;
import dev.tamboui.widgets.scrollbar.ScrollbarState;
import dev.tamboui.text.Text;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Pure, snapshot-only workbench renderer with an immutable pointer hit map. */
final class WorkbenchRenderer {

    private static final int MIN_WIDTH = 88;
    private static final int MIN_HEIGHT = 24;
    private static final int WIDE_WIDTH = 157;
    private static final int HEADER_Y = 0;
    private static final int TAB_BADGES_Y = 1;
    private static final int TAB_LABELS_Y = 2;
    private static final int CONTENT_Y = 3;
    private static final WorkbenchTheme DEFAULT_THEME = WorkbenchTheme.dark();
    private static final Pattern YAML_COMMENT = Pattern.compile("(^|\\s)#.*$");
    private static final Pattern YAML_KEY = Pattern.compile("^(\\s*-?\\s*)([\\w./${}\\-]+)\\s*:");
    private static final Pattern YAML_BOOLEAN_NULL = Pattern.compile(":\\s+(true|false|null)\\s*$");
    private static final Pattern YAML_NUMBER = Pattern.compile(":\\s+(\\d+\\.?\\d*)\\s*$");
    private static final Pattern YAML_STRING_VALUE = Pattern.compile("\"(?:[^\"\\\\]|\\\\.)*\"|'[^']*'");
    private static final Pattern YAML_LIST_MARKER = Pattern.compile("^(\\s*)(-)(\\s)");
    private static final Pattern LOG_CLASS_OR_FRAME = Pattern.compile(
            "(?<![\\w$])(?:[A-Za-z_$][\\w$]*\\.){2,}[A-Za-z_$][\\w$]*(?:\\([^)]*:\\d+\\))?");
    private static final DateTimeFormatter LOG_TIMESTAMP = DateTimeFormatter
            .ofPattern("uuuu-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    private WorkbenchRenderer() {
    }

    static RenderLayout render(Frame frame, WorkbenchState state) {
        return render(frame, state, DEFAULT_THEME);
    }

    static RenderLayout render(Frame frame, WorkbenchState state, WorkbenchTheme theme) {
        return render(frame, state, theme, frame.area(), true, true);
    }

    static RenderLayout render(
            Frame frame, WorkbenchState state, WorkbenchTheme theme, Rect area, boolean renderFooter, boolean sizeGate) {
        Objects.requireNonNull(frame, "frame");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(theme, "theme");
        frame.buffer().setStyle(area, theme.base());
        if (sizeGate && (area.width() < MIN_WIDTH || area.height() < MIN_HEIGHT)) {
            renderTooSmall(frame, area, theme);
            return RenderLayout.forTooSmallFrame();
        }

        boolean wide = area.width() >= WIDE_WIDTH;
        int footerY = renderFooter ? area.bottom() - 1 : area.bottom();
        HeaderLayout header = renderHeaderAndTabs(frame, area, state, wide, theme);
        Rect contentArea = new Rect(
                area.x(), area.y() + CONTENT_Y, area.width(), Math.max(0, footerY - (area.y() + CONTENT_Y)));
        Optional<WorkbenchOverlayState.SourceCreate> sourceCreate = state.overlay()
                .filter(WorkbenchOverlayState.SourceCreate.class::isInstance)
                .map(WorkbenchOverlayState.SourceCreate.class::cast);
        Optional<WorkbenchOverlayState.SourceYamlEditor> sourceYamlEditor = state.overlay()
                .filter(WorkbenchOverlayState.SourceYamlEditor.class::isInstance)
                .map(WorkbenchOverlayState.SourceYamlEditor.class::cast);
        Optional<WorkbenchOverlayState.PipelineCreate> pipelineCreate = state.overlay()
                .filter(WorkbenchOverlayState.PipelineCreate.class::isInstance)
                .map(WorkbenchOverlayState.PipelineCreate.class::cast);
        Optional<WorkbenchOverlayState.PipelineYamlEditor> pipelineYamlEditor = state.overlay()
                .filter(WorkbenchOverlayState.PipelineYamlEditor.class::isInstance)
                .map(WorkbenchOverlayState.PipelineYamlEditor.class::cast);
        ContentLayout content = sourceCreate
                .map(source -> renderSourceCreatePage(frame, contentArea, source, theme))
                .or(() -> sourceYamlEditor.map(editor -> renderSourceYamlEditorPage(
                        frame, contentArea, editor, theme)))
                .or(() -> pipelineCreate.map(pipeline -> renderPipelineCreatePage(
                        frame, contentArea, pipeline, theme)))
                .or(() -> pipelineYamlEditor.map(editor -> renderPipelineYamlEditorPage(
                        frame, contentArea, editor, theme)))
                .orElseGet(() -> renderContent(frame, contentArea, state, wide, theme));
        FooterLayout footer = renderFooter
                ? renderFooter(frame, area, footerY, state, theme)
                : new FooterLayout(List.of());
        List<OverlayHit> overlayHits = state.overlay()
                .filter(overlay -> !(overlay instanceof WorkbenchOverlayState.SourceCreate)
                        && !(overlay instanceof WorkbenchOverlayState.SourceYamlEditor))
                .filter(overlay -> !(overlay instanceof WorkbenchOverlayState.PipelineCreate)
                        && !(overlay instanceof WorkbenchOverlayState.PipelineYamlEditor))
                .map(overlay -> renderOverlay(frame,
                        overlay instanceof WorkbenchOverlayState.Actions ? contentArea : area, overlay, theme))
                .orElseGet(List::of);
        return new RenderLayout(
                false, wide, content.visibleRows(), header.tabHits(), content.rowHits(),
                header.actionHits(), footer.hits(), overlayHits);
    }

    private static void renderTooSmall(Frame frame, Rect area, WorkbenchTheme theme) {
        String title = "Terminal size too small:";
        String actual = "Width = " + area.width() + "  Height = " + area.height();
        String needed = "Needed for current config:";
        String minimum = "Width = " + MIN_WIDTH + "  Height = " + MIN_HEIGHT;
        int startY = area.y() + Math.max(0, (area.height() - 5) / 2);
        writeCentered(frame, area, startY, title, theme.warning().bold());
        writeCentered(frame, area, startY + 1, actual, theme.base());
        writeCentered(frame, area, startY + 3, needed, theme.title());
        writeCentered(frame, area, startY + 4, minimum, theme.base());
    }

    private static HeaderLayout renderHeaderAndTabs(
            Frame frame, Rect area, WorkbenchState state, boolean wide, WorkbenchTheme theme) {
        WorkbenchSessionSnapshot session = state.snapshot()
                .map(WorkbenchSnapshot::session)
                .orElseGet(WorkbenchSessionSnapshot::empty);
        int x = area.x();
        List<TabHit> hits = new ArrayList<>();
        List<ActionHit> actions = new ArrayList<>();
        x += write(frame, x, area.y() + HEADER_Y, " Tapstate", theme.title(), area);
        x += write(frame, x, area.y() + HEADER_Y, "  ", theme.base(), area);
        int contextX = x;
        int contextWidth = write(frame, x, area.y() + HEADER_Y,
                "ctx: " + session.contextName().orElse("-"), theme.info(), area);
        x += contextWidth;
        if (contextWidth > 0) {
            actions.add(new ActionHit(
                    Launcher.CONTEXT,
                    new Rect(contextX, area.y() + HEADER_Y, contextWidth, 1)));
        }
        x += write(frame, x, area.y() + HEADER_Y,
                "  " + connectionMarker(session.connection()) + " " + words(session.connection()),
                connectionStyle(session.connection(), theme), area);
        x += write(frame, x, area.y() + HEADER_Y,
                "  workspace: " + displayPath(session.workspaceRoot()), theme.base(), area);
        x += write(frame, x, area.y() + HEADER_Y, "  ", theme.base(), area);
        int authX = x;
        int authWidth = write(frame, x, area.y() + HEADER_Y,
                "auth: " + displayAuthentication(session), authenticationStyle(session.authentication(), theme), area);
        x += authWidth;
        if (authWidth > 0) {
            actions.add(new ActionHit(
                    Launcher.AUTH,
                    new Rect(authX, area.y() + HEADER_Y, authWidth, 1)));
        }
        if (!session.versions().isBlank()) {
            write(frame, x, area.y() + HEADER_Y, "  " + session.versions(), theme.success(), area);
        }

        String divider = wide ? " | " : "|";
        x = area.x();
        List<WorkbenchState.WorkbenchTab> tabs = headerTabs(area.width(), wide);
        for (int index = 0; index < tabs.size(); index++) {
            WorkbenchState.WorkbenchTab tab = tabs.get(index);
            String label = tabLabel(tab);
            String badge = tabBadge(state, tab);
            if (!badge.isEmpty()) {
                int badgeX = x + Math.max(0, (displayWidth(label) - displayWidth(badge)) / 2);
                write(frame, badgeX, area.y() + TAB_BADGES_Y, badge, theme.info(), area);
            }
            Style style = tab == state.selectedTab()
                    ? theme.accentBackground()
                    : theme.muted();
            int tabStart = x;
            x += write(frame, x, area.y() + TAB_LABELS_Y, tabIcon(tab) + "  ", style, area);
            x += write(frame, x, area.y() + TAB_LABELS_Y, tabNumber(tab), style.underlined(), area);
            x += write(frame, x, area.y() + TAB_LABELS_Y, " " + tabName(tab), style, area);
            int width = x - tabStart;
            if (width > 0) {
                hits.add(new TabHit(tab, new Rect(tabStart, area.y() + TAB_LABELS_Y, width, 1)));
            }
            x += write(frame, x, area.y() + TAB_LABELS_Y, divider, theme.muted(), area);
        }
        int moreX = x;
        String moreLabel = "📂  0 More ▾";
        int moreWidth = write(frame, moreX, area.y() + TAB_LABELS_Y, moreLabel, theme.accent(), area);
        if (moreWidth > 0) {
            actions.add(new ActionHit(
                    Launcher.MORE,
                    new Rect(moreX, area.y() + TAB_LABELS_Y, moreWidth, 1)));
        }
        return new HeaderLayout(List.copyOf(hits), List.copyOf(actions));
    }

    /** Keeps the four primary workbench views and the More launcher visible at the minimum terminal width. */
    private static List<WorkbenchState.WorkbenchTab> headerTabs(int width, boolean wide) {
        List<WorkbenchState.WorkbenchTab> all = List.of(WorkbenchState.WorkbenchTab.values());
        String divider = wide ? " | " : "|";
        int required = displayWidth("📂  0 More ▾");
        for (WorkbenchState.WorkbenchTab tab : all) {
            required += displayWidth(tabIcon(tab) + "  " + tabNumber(tab) + " " + tabName(tab) + divider);
        }
        if (required <= width) {
            return all;
        }
        return List.of(
                WorkbenchState.WorkbenchTab.OVERVIEW,
                WorkbenchState.WorkbenchTab.WORKSPACE,
                WorkbenchState.WorkbenchTab.SOURCES,
                WorkbenchState.WorkbenchTab.PIPELINES);
    }

    private static List<OverlayHit> renderOverlay(
            Frame frame, Rect area, WorkbenchOverlayState overlay, WorkbenchTheme theme) {
        int width = overlay instanceof WorkbenchOverlayState.Actions ? Math.min(40, area.width() - 8)
                : Math.min(60, area.width() - 8);
        int contentRows = switch (overlay) {
            case WorkbenchOverlayState.More ignored -> 3;
            case WorkbenchOverlayState.ContextPicker picker ->
                    Math.max(5, Math.min(10, picker.contexts().size()) + 4);
            case WorkbenchOverlayState.ContextCreate ignored -> 8;
            case WorkbenchOverlayState.SourceCreate source -> source.stage() == WorkbenchOverlayState.SourceCreate.Stage.PREVIEW
                    ? 12 : 9;
            case WorkbenchOverlayState.SourceYamlEditor ignored -> 8;
            case WorkbenchOverlayState.PipelineCreate pipeline -> pipeline.stage() == WorkbenchOverlayState.PipelineCreate.Stage.PREVIEW
                    ? 12 : 9;
            case WorkbenchOverlayState.PipelineYamlEditor ignored -> 8;
            case WorkbenchOverlayState.Confirm ignored -> 4;
            case WorkbenchOverlayState.Login login -> transientLogin(login) ? 7 : 6;
            case WorkbenchOverlayState.Actions actions -> actions.actions().size() + (actions.message().isPresent() ? 2 : 1);
            case WorkbenchOverlayState.LogLevel ignored -> 5;
            case WorkbenchOverlayState.Help ignored -> 6;
        };
        int height = contentRows + 2;
        int x = area.x() + (area.width() - width) / 2;
        int y = overlay instanceof WorkbenchOverlayState.Actions
                ? area.y() + 2 : area.y() + (area.height() - height) / 2;
        Rect box = new Rect(x, y, width, height);
        renderOpaquePopupSurface(frame, box, theme);
        var blockBuilder = Block.builder()
                .borderType(BorderType.ROUNDED)
                .borders(Borders.ALL)
                .title(Title.from(Line.from(Span.styled(
                        " " + overlayTitle(overlay) + " ", theme.title()))));
        if (!(overlay instanceof WorkbenchOverlayState.Actions)
                && !(overlay instanceof WorkbenchOverlayState.Confirm)) {
            blockBuilder.borderStyle(theme.accent());
        }
        Block block = blockBuilder.build();
        frame.renderWidget(block, box);

        return switch (overlay) {
            case WorkbenchOverlayState.More more -> renderMore(frame, area, box, more, theme);
            case WorkbenchOverlayState.ContextPicker picker ->
                    renderContexts(frame, area, box, picker, theme);
            case WorkbenchOverlayState.ContextCreate create ->
                    renderContextCreate(frame, area, box, create, theme);
            case WorkbenchOverlayState.SourceCreate source -> renderSourceCreate(frame, area, box, source, theme);
            case WorkbenchOverlayState.SourceYamlEditor editor -> renderSourceYamlEditor(
                    frame, area, box, editor, theme);
            case WorkbenchOverlayState.PipelineCreate pipeline -> renderPipelineCreate(frame, area, box, pipeline, theme);
            case WorkbenchOverlayState.PipelineYamlEditor editor -> renderPipelineYamlEditor(
                    frame, area, box, editor, theme);
            case WorkbenchOverlayState.Confirm confirm -> renderConfirm(frame, area, box, confirm, theme);
            case WorkbenchOverlayState.Login login -> renderLogin(frame, area, box, login, theme);
            case WorkbenchOverlayState.Actions actions -> renderActions(frame, area, box, actions, theme);
            case WorkbenchOverlayState.LogLevel level -> renderLogLevel(frame, area, box, level, theme);
            case WorkbenchOverlayState.Help ignored -> renderHelp(frame, area, box, theme);
        };
    }

    private static String overlayTitle(WorkbenchOverlayState overlay) {
        return switch (overlay) {
            case WorkbenchOverlayState.More ignored -> "More";
            case WorkbenchOverlayState.ContextPicker ignored -> "Choose Context";
            case WorkbenchOverlayState.ContextCreate ignored -> "New Context";
            case WorkbenchOverlayState.SourceCreate ignored -> "New Source";
            case WorkbenchOverlayState.SourceYamlEditor ignored -> "Edit YAML";
            case WorkbenchOverlayState.PipelineCreate ignored -> "New Pipeline";
            case WorkbenchOverlayState.PipelineYamlEditor ignored -> "Edit YAML";
            case WorkbenchOverlayState.Confirm confirm -> confirm.title();
            case WorkbenchOverlayState.Login login -> "Sign in to " + login.contextName();
            case WorkbenchOverlayState.Actions ignored -> "Actions";
            case WorkbenchOverlayState.LogLevel ignored -> "Log level";
            case WorkbenchOverlayState.Help ignored -> "Help";
        };
    }

    private static List<OverlayHit> renderLogLevel(
            Frame frame, Rect area, Rect box, WorkbenchOverlayState.LogLevel level, WorkbenchTheme theme) {
        List<OverlayHit> hits = new ArrayList<>();
        for (int index = 0; index < WorkbenchOverlayState.LogLevel.LEVELS.size(); index++) {
            boolean selected = index == level.selectedIndex();
            int y = box.y() + 1 + index;
            String text = (selected ? "> " : "  ") + WorkbenchOverlayState.LogLevel.LEVELS.get(index);
            int width = write(frame, box.x() + 2, y, text, selected ? theme.selection() : theme.base(), area);
            hits.add(new OverlayHit(index, new Rect(box.x() + 2, y, width, 1)));
        }
        return List.copyOf(hits);
    }

    private static List<OverlayHit> renderMore(
            Frame frame, Rect area, Rect box, WorkbenchOverlayState.More more, WorkbenchTheme theme) {
        List<String> entries = List.of("Context", "Authentication", "Help");
        List<OverlayHit> hits = new ArrayList<>();
        for (int index = 0; index < entries.size(); index++) {
            String line = (index == more.selectedIndex() ? "> " : "  ") + entries.get(index);
            int rowY = box.y() + 1 + index;
            int width = write(frame, box.x() + 2, rowY, line,
                    index == more.selectedIndex() ? theme.selection() : theme.base(), area);
            hits.add(new OverlayHit(index, new Rect(box.x() + 2, rowY, width, 1)));
        }
        return List.copyOf(hits);
    }

    private static List<OverlayHit> renderContexts(
            Frame frame,
            Rect area,
            Rect box,
            WorkbenchOverlayState.ContextPicker picker,
            WorkbenchTheme theme) {
        List<OverlayHit> hits = new ArrayList<>();
        int visible = Math.min(10, picker.contexts().size());
        for (int index = 0; index < visible; index++) {
            WorkbenchActionGateway.ContextOption context = picker.contexts().get(index);
            String line = (index == picker.selectedIndex() ? "> " : "  ")
                    + context.name() + (context.suggested() ? "  suggested" : "");
            int rowY = box.y() + 1 + index;
            int width = write(frame, box.x() + 2, rowY, line,
                    index == picker.selectedIndex() ? theme.selection() : theme.base(), area);
            hits.add(new OverlayHit(index, new Rect(box.x() + 2, rowY, width, 1)));
        }
        int createIndex = picker.contexts().size();
        int createY = box.y() + 1 + visible;
        String createLine = (createIndex == picker.selectedIndex() ? "> " : "  ") + "+ New Context";
        int createWidth = write(frame, box.x() + 2, createY, createLine,
                createIndex == picker.selectedIndex() ? theme.selection() : theme.accent(), area);
        hits.add(new OverlayHit(createIndex, new Rect(box.x() + 2, createY, createWidth, 1)));
        picker.message().ifPresent(message -> write(
                frame, box.x() + 2, box.y() + box.height() - 3, message,
                picker.pending() ? theme.info() : theme.warning(), area));
        return List.copyOf(hits);
    }

    private static List<OverlayHit> renderContextCreate(
            Frame frame,
            Rect area,
            Rect box,
            WorkbenchOverlayState.ContextCreate create,
            WorkbenchTheme theme) {
        renderFormField(frame, area, box.x() + 2, box.y() + 1, "Name", create.name(),
                create.stage() == WorkbenchOverlayState.ContextCreate.Stage.NAME, theme);
        renderFormField(frame, area, box.x() + 2, box.y() + 2, "Server", create.server(),
                create.stage() == WorkbenchOverlayState.ContextCreate.Stage.SERVER, theme);
        renderFormField(frame, area, box.x() + 2, box.y() + 3, "Verify TLS",
                create.verifyTls() ? "Yes" : "No",
                create.stage() == WorkbenchOverlayState.ContextCreate.Stage.VERIFY_TLS, theme);
        String hint = create.pending()
                ? "Creating context..."
                : create.stage() == WorkbenchOverlayState.ContextCreate.Stage.VERIFY_TLS
                        ? "Y/N or Space toggle  Enter create  Esc cancel"
                        : "Enter next  Esc cancel";
        write(frame, box.x() + 2, box.y() + 5, hint, theme.muted(), area);
        create.message().ifPresent(message -> write(
                frame, box.x() + 2, box.y() + 6, message, theme.error(), area));
        return List.of();
    }

    private static List<OverlayHit> renderSourceCreate(
            Frame frame,
            Rect area,
            Rect box,
            WorkbenchOverlayState.SourceCreate source,
            WorkbenchTheme theme) {
        if (source.stage() == WorkbenchOverlayState.SourceCreate.Stage.PREVIEW) {
            write(frame, box.x() + 2, box.y() + 1, "Canonical YAML preview", theme.label().bold(), area);
            String[] lines = source.canonicalYaml().orElse("").split("\\R");
            for (int index = 0; index < Math.min(7, lines.length); index++) {
                write(frame, box.x() + 2, box.y() + 2 + index, lines[index], theme.base(), area);
            }
            write(frame, box.x() + 2, box.y() + box.height() - 3,
                    source.pending() ? "Creating source..." : "Enter create  Esc back", theme.muted(), area);
            source.message().ifPresent(message -> write(
                    frame, box.x() + 2, box.y() + box.height() - 2, message, theme.error(), area));
            return List.of();
        }
        String label = switch (source.stage()) {
            case CONNECTOR -> "Connector";
            case MODE -> "Read mode";
            case TABLES -> "Tables";
            case CONFIG -> "Configuration";
            case ID -> "Resource id";
            case PREVIEW -> throw new IllegalStateException("preview handled above");
        };
        String value = switch (source.stage()) {
            case CONNECTOR -> source.connector();
            case MODE -> source.mode();
            case TABLES -> source.tables();
            case CONFIG -> "";
            case ID -> source.id();
            case PREVIEW -> "";
        };
        renderFormField(frame, area, box.x() + 2, box.y() + 1, label, value, true, theme);
        if (source.stage() == WorkbenchOverlayState.SourceCreate.Stage.CONNECTOR
                || source.stage() == WorkbenchOverlayState.SourceCreate.Stage.MODE) {
            List<String> options = source.stage() == WorkbenchOverlayState.SourceCreate.Stage.CONNECTOR
                    ? source.catalog().connectors().stream().map(WorkbenchActionGateway.SourceConnector::id)
                            .filter(id -> id.toLowerCase(Locale.ROOT)
                                    .contains(source.filter().toLowerCase(Locale.ROOT))).toList()
                    : source.catalog().connectors().stream()
                            .filter(connector -> connector.id().equals(source.connector()))
                            .findFirst().map(WorkbenchActionGateway.SourceConnector::modes).orElse(List.of());
            int visible = Math.min(4, options.size());
            int first = Math.clamp(source.selectedIndex() - visible / 2, 0, Math.max(0, options.size() - visible));
            for (int offset = 0; offset < visible; offset++) {
                int index = first + offset;
                boolean selected = index == source.selectedIndex();
                write(frame, box.x() + 4, box.y() + 3 + offset,
                        (selected ? "  " : "  ") + options.get(index),
                        selected ? theme.selection() : theme.base(), area);
            }
        } else {
            write(frame, box.x() + 2, box.y() + 3,
                    source.stage() == WorkbenchOverlayState.SourceCreate.Stage.TABLES
                            ? "Comma-separated names; /regex/ is supported; blank means all."
                            : source.stage() == WorkbenchOverlayState.SourceCreate.Stage.CONFIG
                                    ? "Up/Down fields  Left/Right choices  Enter next"
                            : "Choose a stable local identifier.", theme.muted(), area);
        }
        write(frame, box.x() + 2, box.y() + box.height() - 3,
                source.pending() ? "Loading source catalog..." : "Up/Down navigate  Enter next  Esc cancel",
                theme.muted(), area);
        source.message().ifPresent(message -> write(
                frame, box.x() + 2, box.y() + box.height() - 2, message, theme.error(), area));
        return List.of();
    }

    private static List<OverlayHit> renderSourceYamlEditor(
            Frame frame,
            Rect area,
            Rect box,
            WorkbenchOverlayState.SourceYamlEditor editor,
            WorkbenchTheme theme) {
        renderDocument(frame, new Rect(box.x() + 2, box.y() + 1,
                Math.max(1, box.width() - 4), Math.max(1, box.height() - 2)),
                editor.document(), theme);
        return List.of();
    }

    private static List<OverlayHit> renderPipelineCreate(
            Frame frame, Rect area, Rect box, WorkbenchOverlayState.PipelineCreate pipeline, WorkbenchTheme theme) {
        renderFormField(frame, area, box.x() + 2, box.y() + 1,
                pipeline.stage() == WorkbenchOverlayState.PipelineCreate.Stage.SOURCE ? "Source" : "Pipeline id",
                pipeline.stage() == WorkbenchOverlayState.PipelineCreate.Stage.SOURCE ? pipeline.sourceId() : pipeline.id(),
                true, theme);
        if (pipeline.stage() == WorkbenchOverlayState.PipelineCreate.Stage.SOURCE) {
            int visible = Math.min(4, pipeline.sourceIds().size());
            for (int index = 0; index < visible; index++) {
                write(frame, box.x() + 4, box.y() + 3 + index, pipeline.sourceIds().get(index),
                        index == pipeline.selectedIndex() ? theme.selection() : theme.base(), area);
            }
        } else if (pipeline.stage() == WorkbenchOverlayState.PipelineCreate.Stage.PREVIEW) {
            String[] lines = pipeline.canonicalYaml().orElse("").split("\\R");
            for (int index = 0; index < Math.min(7, lines.length); index++) {
                write(frame, box.x() + 2, box.y() + 2 + index, lines[index], theme.base(), area);
            }
        }
        pipeline.message().ifPresent(message -> write(frame, box.x() + 2, box.y() + box.height() - 2,
                message, theme.error(), area));
        return List.of();
    }

    private static List<OverlayHit> renderPipelineYamlEditor(
            Frame frame, Rect area, Rect box, WorkbenchOverlayState.PipelineYamlEditor editor, WorkbenchTheme theme) {
        renderDocument(frame, new Rect(box.x() + 1, box.y() + 1, box.width() - 2, box.height() - 2),
                editor.document(), theme);
        return List.of();
    }

    private static ContentLayout renderSourceCreatePage(
            Frame frame,
            Rect area,
            WorkbenchOverlayState.SourceCreate source,
            WorkbenchTheme theme) {
        Block block = Block.builder()
                .borderType(BorderType.ROUNDED)
                .borders(Borders.ALL)
                .borderStyle(theme.base())
                .title(Title.from(Line.from(Span.styled(" ✨ New Source ", theme.title()))))
                .build();
        frame.renderWidget(block, area);
        Rect inner = block.inner(area);
        boolean configStage = source.stage() == WorkbenchOverlayState.SourceCreate.Stage.CONFIG;
        boolean stacked = configStage && inner.width() < 120;
        int leftWidth = stacked
                ? inner.width()
                : configStage ? Math.clamp(inner.width() / 2, 60, 82) : Math.clamp(inner.width() / 2, 34, 50);
        int formHeight = stacked ? Math.max(1, inner.height() * 2 / 3) : inner.height();
        Rect formArea = new Rect(inner.x(), inner.y(), leftWidth, formHeight);
        Rect previewArea = stacked
                ? new Rect(inner.x(), inner.y() + formHeight + 1,
                        inner.width(), Math.max(0, inner.height() - formHeight - 1))
                : new Rect(inner.x() + leftWidth + 1, inner.y(),
                        Math.max(0, inner.width() - leftWidth - 1), inner.height());
        int y = formArea.y();
        List<WorkbenchActionGateway.SourceConfigField> configFields = sourceConfigFields(source);
        if (stacked) {
            write(frame, formArea.x(), y++, "Configuration · " + source.connector() + " · " + source.mode(),
                    theme.label().bold(), formArea);
            y++;
        } else {
            write(frame, formArea.x(), y++, "Guided authoring", theme.label().bold(), formArea);
            y++;
            renderSourcePageField(frame, formArea, y++, "1", "Connector", source.connector(),
                    source.stage() == WorkbenchOverlayState.SourceCreate.Stage.CONNECTOR, theme);
            renderSourcePageField(frame, formArea, y++, "2", "Read mode", source.mode(),
                    source.stage() == WorkbenchOverlayState.SourceCreate.Stage.MODE, theme);
            renderSourcePageField(frame, formArea, y++, "3", "Tables", source.tables().isBlank() ? "All tables" : source.tables(),
                    source.stage() == WorkbenchOverlayState.SourceCreate.Stage.TABLES, theme);
            renderSourcePageField(frame, formArea, y++, "4", "Configuration",
                    configFields.isEmpty() ? "No connector fields" : configFields.size() + " fields",
                    source.stage() == WorkbenchOverlayState.SourceCreate.Stage.CONFIG, theme);
            renderSourcePageField(frame, formArea, y++, "5", "Resource id", source.id(),
                    source.stage() == WorkbenchOverlayState.SourceCreate.Stage.ID, theme);
            y++;
        }
        if (source.stage() == WorkbenchOverlayState.SourceCreate.Stage.CONNECTOR
                || source.stage() == WorkbenchOverlayState.SourceCreate.Stage.MODE) {
            List<String> options = source.stage() == WorkbenchOverlayState.SourceCreate.Stage.CONNECTOR
                    ? source.catalog().connectors().stream().map(WorkbenchActionGateway.SourceConnector::id)
                            .filter(id -> id.toLowerCase(Locale.ROOT)
                                    .contains(source.filter().toLowerCase(Locale.ROOT))).toList()
                    : source.catalog().connectors().stream()
                            .filter(connector -> connector.id().equals(source.connector()))
                            .findFirst().map(WorkbenchActionGateway.SourceConnector::modes).orElse(List.of());
            if (options.isEmpty()) {
                write(frame, formArea.x() + 2, y, source.pending() ? "Loading connectors..." : "No matching connectors.",
                        theme.muted(), formArea);
            } else {
                int visible = Math.max(1, Math.min(options.size(), formArea.bottom() - y - 2));
                int first = Math.clamp(source.selectedIndex() - visible / 2, 0, Math.max(0, options.size() - visible));
                for (int offset = 0; offset < visible; offset++) {
                    int index = first + offset;
                    write(frame, formArea.x() + 2, y + offset, pad(options.get(index), formArea.width() - 2),
                            index == source.selectedIndex() ? theme.selection() : theme.base(), formArea);
                }
            }
            write(frame, formArea.x(), formArea.bottom() - 1,
                    source.stage() == WorkbenchOverlayState.SourceCreate.Stage.CONNECTOR
                            ? source.catalog().connectors().size() + " connectors  Filter: " + source.filter() + "  ↑↓ navigate"
                            : "↑↓ navigate", theme.muted(), formArea);
        } else if (source.stage() == WorkbenchOverlayState.SourceCreate.Stage.CONFIG) {
            renderSourceConfigFields(frame, formArea, y, source, configFields, theme);
        } else {
            String hint = switch (source.stage()) {
                case TABLES -> "Comma-separated names, /regex/, or leave blank for all.";
                case CONFIG -> "Configure the selected connector field.";
                case ID -> "Choose a stable local identifier.";
                case PREVIEW -> "Review the canonical artifact before creating it.";
                default -> "";
            };
            write(frame, formArea.x(), y, hint, theme.muted(), formArea);
        }
        if (previewArea.width() >= 4 && previewArea.height() >= 3) {
            Block previewBlock = Block.builder()
                    .borderType(BorderType.ROUNDED)
                    .borders(Borders.ALL)
                    .borderStyle(source.stage() == WorkbenchOverlayState.SourceCreate.Stage.PREVIEW
                            ? theme.accent() : theme.base())
                    .title(Title.from(Line.from(Span.styled(" Canonical YAML ", theme.title()))))
                    .build();
            frame.renderWidget(previewBlock, previewArea);
            Rect previewInner = previewBlock.inner(previewArea);
            String yaml = source.canonicalYaml().orElse(source.pending()
                    ? "Generating live draft preview..."
                    : "Live YAML preview will appear here.");
            String[] lines = yaml.split("\\R");
            for (int index = 0; index < Math.min(lines.length, previewInner.height()); index++) {
                write(frame, previewInner.x(), previewInner.y() + index, lines[index], theme.base(), previewInner);
            }
        }
        source.message().ifPresent(message -> write(
                frame, formArea.x(), formArea.bottom() - 1, message, theme.error(), formArea));
        return new ContentLayout(List.of(), Math.max(1, formArea.height()));
    }

    private static ContentLayout renderSourceYamlEditorPage(
            Frame frame,
            Rect area,
            WorkbenchOverlayState.SourceYamlEditor editor,
            WorkbenchTheme theme) {
        String title = " ✎ Edit source/" + editor.source().id() + ".tap.yml"
                + (editor.document().dirty() ? " *" : "") + " ";
        Block block = Block.builder()
                .borderType(BorderType.ROUNDED)
                .borders(Borders.ALL)
                .borderStyle(theme.accent())
                .title(Title.from(Line.from(Span.styled(title, theme.title()))))
                .build();
        frame.renderWidget(block, area);
        renderDocument(frame, block.inner(area), editor.document(), theme);
        return new ContentLayout(List.of(), Math.max(1, block.inner(area).height()));
    }

    private static ContentLayout renderPipelineCreatePage(
            Frame frame,
            Rect area,
            WorkbenchOverlayState.PipelineCreate pipeline,
            WorkbenchTheme theme) {
        Block block = Block.builder()
                .borderType(BorderType.ROUNDED)
                .borders(Borders.ALL)
                .borderStyle(theme.base())
                .title(Title.from(Line.from(Span.styled(" ⚡ New Pipeline ", theme.title()))))
                .build();
        frame.renderWidget(block, area);
        Rect inner = block.inner(area);
        int leftWidth = Math.clamp(inner.width() / 2, 38, 58);
        Rect formArea = new Rect(inner.x(), inner.y(), leftWidth, inner.height());
        Rect previewArea = new Rect(inner.x() + leftWidth + 1, inner.y(),
                Math.max(0, inner.width() - leftWidth - 1), inner.height());
        int y = formArea.y();
        write(frame, formArea.x(), y++, "Guided authoring", theme.label().bold(), formArea);
        y++;
        renderSourcePageField(frame, formArea, y++, "1", "Source", pipeline.sourceId(),
                pipeline.stage() == WorkbenchOverlayState.PipelineCreate.Stage.SOURCE, theme);
        renderSourcePageField(frame, formArea, y++, "2", "Pipeline id", pipeline.id(),
                pipeline.stage() == WorkbenchOverlayState.PipelineCreate.Stage.ID, theme);
        y++;
        if (pipeline.stage() == WorkbenchOverlayState.PipelineCreate.Stage.SOURCE) {
            int visible = Math.max(1, Math.min(pipeline.sourceIds().size(), formArea.bottom() - y - 2));
            int first = Math.clamp(pipeline.selectedIndex() - visible / 2, 0,
                    Math.max(0, pipeline.sourceIds().size() - visible));
            for (int offset = 0; offset < visible; offset++) {
                int index = first + offset;
                write(frame, formArea.x() + 2, y + offset,
                        pad(pipeline.sourceIds().get(index), formArea.width() - 2),
                        index == pipeline.selectedIndex() ? theme.selection() : theme.base(), formArea);
            }
            write(frame, formArea.x(), formArea.bottom() - 1, "↑↓ choose source", theme.muted(), formArea);
        } else {
            String hint = switch (pipeline.stage()) {
                case ID -> "Choose a stable local identifier.";
                case PREVIEW -> "F4 edits the complete Pipeline graph.";
                default -> "";
            };
            write(frame, formArea.x(), y, hint, theme.muted(), formArea);
        }
        if (previewArea.width() >= 4 && previewArea.height() >= 3) {
            Block previewBlock = Block.builder()
                    .borderType(BorderType.ROUNDED)
                    .borders(Borders.ALL)
                    .borderStyle(pipeline.stage() == WorkbenchOverlayState.PipelineCreate.Stage.PREVIEW
                            ? theme.accent() : theme.base())
                    .title(Title.from(Line.from(Span.styled(" Canonical YAML ", theme.title()))))
                    .build();
            frame.renderWidget(previewBlock, previewArea);
            Rect previewInner = previewBlock.inner(previewArea);
            String yaml = pipeline.canonicalYaml().orElse(pipeline.pending()
                    ? "Generating Pipeline preview..." : "Preview will include a default inline View.");
            String[] lines = yaml.split("\\R");
            for (int index = 0; index < Math.min(lines.length, previewInner.height()); index++) {
                write(frame, previewInner.x(), previewInner.y() + index, lines[index], theme.base(), previewInner);
            }
        }
        pipeline.message().ifPresent(message -> write(frame, formArea.x(), formArea.bottom() - 1,
                message, theme.error(), formArea));
        return new ContentLayout(List.of(), Math.max(1, formArea.height()));
    }

    private static ContentLayout renderPipelineYamlEditorPage(
            Frame frame,
            Rect area,
            WorkbenchOverlayState.PipelineYamlEditor editor,
            WorkbenchTheme theme) {
        String title = " ✎ Edit pipeline/" + editor.pipeline().id() + ".tap.yml"
                + (editor.document().dirty() ? " *" : "") + " ";
        Block block = Block.builder()
                .borderType(BorderType.ROUNDED)
                .borders(Borders.ALL)
                .borderStyle(theme.accent())
                .title(Title.from(Line.from(Span.styled(title, theme.title()))))
                .build();
        frame.renderWidget(block, area);
        renderDocument(frame, block.inner(area), editor.document(), theme);
        return new ContentLayout(List.of(), Math.max(1, block.inner(area).height()));
    }

    private static void renderSourcePageField(
            Frame frame,
            Rect area,
            int y,
            String step,
            String label,
            String value,
            boolean active,
            WorkbenchTheme theme) {
        Style labelStyle = active ? theme.title().bold() : theme.muted();
        int x = area.x();
        x += write(frame, x, y, step + " ", active ? theme.accent() : theme.muted(), area);
        x += write(frame, x, y, label + ": ", labelStyle, area);
        write(frame, x, y, value, active ? theme.base().bold() : theme.base(), area);
    }

    private static List<WorkbenchActionGateway.SourceConfigField> sourceConfigFields(
            WorkbenchOverlayState.SourceCreate source) {
        return source.catalog().connectors().stream()
                .filter(connector -> connector.id().equals(source.connector()))
                .findFirst().map(WorkbenchActionGateway.SourceConnector::configFields).orElse(List.of()).stream()
                .filter(field -> field.visibleWhen().map(visibility -> {
                    String controller = source.config().get(visibility.controllingField());
                    return controller != null && visibility.equalsAnyOf().contains(controller);
                }).orElse(true))
                .toList();
    }

    private static void renderSourceConfigFields(
            Frame frame, Rect area, int y, WorkbenchOverlayState.SourceCreate source,
            List<WorkbenchActionGateway.SourceConfigField> fields, WorkbenchTheme theme) {
        if (fields.isEmpty()) {
            write(frame, area.x() + 2, y, "No visible connector fields.", theme.muted(), area);
            return;
        }
        int contentWidth = Math.max(1, area.width() - 2);
        int availableRows = Math.max(1, area.bottom() - y - 2);
        int selected = Math.clamp(source.selectedIndex(), 0, fields.size() - 1);
        int[] rowHeights = new int[fields.size()];
        for (int index = 0; index < fields.size(); index++) {
            rowHeights[index] = sourceConfigRowHeight(fields.get(index), source, contentWidth);
        }
        int selectedRow = 0;
        for (int index = 0; index < selected; index++) {
            selectedRow += rowHeights[index];
        }
        int first = 0;
        int firstRow = 0;
        while (first < selected && selectedRow - firstRow >= availableRows) {
            firstRow += rowHeights[first++];
        }
        int row = 0;
        for (int index = first; index < fields.size() && row < availableRows; index++) {
            WorkbenchActionGateway.SourceConfigField field = fields.get(index);
            boolean active = index == selected;
            if (!field.options().isEmpty()) {
                row += renderSourceConfigOption(frame, area, y + row, source, field, theme, active, contentWidth);
                continue;
            }
            String value = source.config().getOrDefault(field.name(), "");
            if (field.secret() && !value.isBlank()) {
                value = "*".repeat(value.codePointCount(0, value.length()));
            }
            if (value.isBlank() && field.defaultValue() != null && !field.defaultValue().isBlank()) {
                value = "[default: " + field.defaultValue() + "]";
            }
            write(frame, area.x() + 2, y + row, pad(field.label() + ": " + value, contentWidth),
                    active ? theme.selection() : theme.base(), area);
            row++;
        }
        write(frame, area.x(), area.bottom() - 1,
                "↑↓ fields  ←→ choices  type value  " + (selected + 1) + "/" + fields.size(), theme.muted(), area);
    }

    private static int sourceConfigRowHeight(
            WorkbenchActionGateway.SourceConfigField field,
            WorkbenchOverlayState.SourceCreate source,
            int width) {
        if (field.options().isEmpty()) {
            return 1;
        }
        String value = effectiveSourceConfigValue(field, source);
        List<List<WorkbenchActionGateway.SourceConfigOption>> lines = sourceConfigOptionLines(field, value, width);
        return lines.size() == 1 ? 1 : 1 + lines.size();
    }

    private static int renderSourceConfigOption(
            Frame frame, Rect area, int y, WorkbenchOverlayState.SourceCreate source,
            WorkbenchActionGateway.SourceConfigField field, WorkbenchTheme theme, boolean active, int width) {
        String value = effectiveSourceConfigValue(field, source);
        List<List<WorkbenchActionGateway.SourceConfigOption>> lines = sourceConfigOptionLines(field, value, width);
        if (lines.size() == 1) {
            List<Span> spans = sourceConfigOptionSpans(field, value, lines.getFirst(), active, theme, true);
            frame.renderWidget(Paragraph.from(Line.from(spans)), new Rect(
                    area.x() + 2, y, width, 1));
            return 1;
        }
        write(frame, area.x() + 2, y, field.label() + ":", active ? theme.title().bold() : theme.muted(), area);
        for (int index = 0; index < lines.size(); index++) {
            frame.renderWidget(Paragraph.from(Line.from(sourceConfigOptionSpans(
                    field, value, lines.get(index), active, theme, false))), new Rect(
                    area.x() + 2, y + index + 1, width, 1));
        }
        return 1 + lines.size();
    }

    private static String effectiveSourceConfigValue(
            WorkbenchActionGateway.SourceConfigField field,
            WorkbenchOverlayState.SourceCreate source) {
        String configured = source.config().get(field.name());
        if (configured != null && field.options().stream().anyMatch(option -> option.value().equals(configured))) {
            return configured;
        }
        if (field.defaultValue() != null
                && field.options().stream().anyMatch(option -> option.value().equals(field.defaultValue()))) {
            return field.defaultValue();
        }
        return field.options().getFirst().value();
    }

    private static List<List<WorkbenchActionGateway.SourceConfigOption>> sourceConfigOptionLines(
            WorkbenchActionGateway.SourceConfigField field, String value, int width) {
        int inlineWidth = displayWidth(field.label() + ": ");
        for (int index = 0; index < field.options().size(); index++) {
            if (index > 0) {
                inlineWidth++;
            }
            inlineWidth += displayWidth(sourceConfigOptionText(field.options().get(index), value));
        }
        if (inlineWidth + 4 <= width) {
            return List.of(field.options());
        }
        List<List<WorkbenchActionGateway.SourceConfigOption>> lines = new ArrayList<>();
        List<WorkbenchActionGateway.SourceConfigOption> line = new ArrayList<>();
        int lineWidth = 0;
        for (WorkbenchActionGateway.SourceConfigOption option : field.options()) {
            int optionWidth = displayWidth(sourceConfigOptionText(option, value));
            int requiredWidth = line.isEmpty() ? optionWidth : lineWidth + 1 + optionWidth;
            if (!line.isEmpty() && requiredWidth > width) {
                lines.add(List.copyOf(line));
                line.clear();
                lineWidth = 0;
            }
            if (!line.isEmpty()) {
                lineWidth++;
            }
            line.add(option);
            lineWidth += optionWidth;
        }
        if (!line.isEmpty()) {
            lines.add(List.copyOf(line));
        }
        return List.copyOf(lines);
    }

    private static List<Span> sourceConfigOptionSpans(
            WorkbenchActionGateway.SourceConfigField field,
            String value,
            List<WorkbenchActionGateway.SourceConfigOption> options,
            boolean active,
            WorkbenchTheme theme,
            boolean includeLabel) {
        List<Span> spans = new ArrayList<>();
        if (includeLabel) {
            spans.add(Span.styled(field.label() + ": ", active ? theme.title().bold() : theme.muted()));
        }
        for (int index = 0; index < options.size(); index++) {
            if (index > 0) {
                spans.add(Span.styled(" ", theme.base()));
            }
            WorkbenchActionGateway.SourceConfigOption option = options.get(index);
            boolean selected = option.value().equals(value);
            spans.add(Span.styled(sourceConfigOptionText(option, value),
                    selected ? theme.base().bold() : theme.muted()));
        }
        return List.copyOf(spans);
    }

    private static String sourceConfigOptionText(
            WorkbenchActionGateway.SourceConfigOption option, String value) {
        return option.value().equals(value) ? "[" + option.label() + "]" : " " + option.label() + " ";
    }

    private static List<OverlayHit> renderConfirm(
            Frame frame,
            Rect area,
            Rect box,
            WorkbenchOverlayState.Confirm confirm,
            WorkbenchTheme theme) {
        write(frame, box.x() + 2, box.y() + 1, confirm.message(), theme.warning(), area);
        if (confirm.pending()) {
            write(frame, box.x() + 2, box.y() + 2, "Working...", theme.muted(), area);
        }
        return List.of();
    }

    private static void renderFormField(
            Frame frame,
            Rect area,
            int x,
            int y,
            String label,
            String value,
            boolean active,
            WorkbenchTheme theme) {
        int next = x + write(frame, x, y, active ? "> " : "  ", theme.muted(), area);
        next += write(frame, next, y, label + ":", active ? theme.label().bold() : theme.muted(), area);
        write(frame, next, y, " " + value, theme.base(), area);
    }

    private static List<OverlayHit> renderLogin(
            Frame frame, Rect area, Rect box, WorkbenchOverlayState.Login login, WorkbenchTheme theme) {
        boolean transientLogin = transientLogin(login);
        int usernameY = box.y() + (transientLogin ? 2 : 1);
        int passwordY = usernameY + 1;
        int hintY = passwordY + 1;
        int messageY = hintY + 1;
        if (transientLogin) {
            renderLoginField(frame, area, box.x() + 2, box.y() + 1, "Server", login.server(),
                    login.stage() == WorkbenchOverlayState.Login.Stage.SERVER, theme);
        }
        renderLoginField(frame, area, box.x() + 2, usernameY, "Username", login.username(),
                login.stage() == WorkbenchOverlayState.Login.Stage.USERNAME, theme);
        renderLoginField(frame, area, box.x() + 2, passwordY, "Password", login.password().mask(),
                login.stage() == WorkbenchOverlayState.Login.Stage.PASSWORD, theme);
        String hint = login.pending()
                ? "Signing in..."
                : login.stage() != WorkbenchOverlayState.Login.Stage.PASSWORD
                        ? "Enter next  Esc cancel"
                        : "Enter sign in  Esc cancel";
        write(frame, box.x() + 2, hintY, hint, theme.muted(), area);
        login.message().ifPresent(message -> write(
                frame, box.x() + 2, messageY, message, theme.error(), area));
        return List.of();
    }

    private static List<OverlayHit> renderActions(
            Frame frame, Rect area, Rect box, WorkbenchOverlayState.Actions actions, WorkbenchTheme theme) {
        List<OverlayHit> hits = new ArrayList<>();
        for (int index = 0; index < actions.actions().size(); index++) {
            WorkbenchOverlayState.Actions.Action action = actions.actions().get(index);
            int rowY = box.y() + 1 + index;
            boolean selected = index == actions.selectedIndex();
            String line = "  " + action.label();
            int rowWidth = Math.max(0, box.width() - 2);
            write(frame, box.x() + 1, rowY, pad(line, rowWidth),
                    selected ? theme.selection() : theme.base(), area);
            hits.add(new OverlayHit(index, new Rect(box.x() + 1, rowY, rowWidth, 1)));
        }
        actions.message().ifPresent(message -> write(frame, box.x() + 2, box.bottom() - 2,
                clip(message, Math.max(0, box.width() - 4)), theme.warning(), area));
        return List.copyOf(hits);
    }

    private static void renderLoginField(
            Frame frame, Rect area, int x, int y, String label, String value,
            boolean active, WorkbenchTheme theme) {
        renderFormField(frame, area, x, y, label, value, active, theme);
    }

    private static boolean transientLogin(WorkbenchOverlayState.Login login) {
        return login.stage() == WorkbenchOverlayState.Login.Stage.SERVER || !login.server().isBlank();
    }

    private static List<OverlayHit> renderHelp(
            Frame frame, Rect area, Rect box, WorkbenchTheme theme) {
        write(frame, box.x() + 2, box.y() + 1, "1-4 switch views", theme.base(), area);
        write(frame, box.x() + 2, box.y() + 2, "c context   a authentication", theme.base(), area);
        write(frame, box.x() + 2, box.y() + 3,
                "r refresh   q quit (confirm)   Esc close", theme.base(), area);
        return List.of();
    }

    private static ContentLayout renderContent(
            Frame frame,
            Rect area,
            WorkbenchState state,
            boolean wide,
            WorkbenchTheme theme) {
        if (state.selectedTab() == WorkbenchState.WorkbenchTab.WORKSPACE
                && state.snapshot().isPresent()) {
            return renderWorkspace(frame, area, state, state.snapshot().orElseThrow(), theme);
        }
        if (state.selectedTab() == WorkbenchState.WorkbenchTab.LOGS) {
            renderLogs(frame, area, state.logs(), theme);
            return new ContentLayout(List.of(), Math.max(1, area.height() - 2));
        }
        if (state.selectedTab() == WorkbenchState.WorkbenchTab.INSPECT) {
            renderInspect(frame, area, state, theme);
            return new ContentLayout(List.of(), Math.max(1, area.height() - 2));
        }
        Block block = Block.builder()
                .borderType(BorderType.ROUNDED)
                .borders(Borders.ALL)
                .borderStyle(theme.base())
                .title(Title.from(Line.from(Span.styled(
                        " " + state.selectedTab().label() + " ", theme.title()))))
                .build();
        frame.renderWidget(block, area);
        Rect inner = block.inner(area);
        int visibleRows = Math.max(1, inner.height() - 2);
        if (state.snapshot().isEmpty()) {
            write(frame, inner.x(), inner.y(), state.selectedTab().emptyMessage(), theme.base(), inner);
            write(frame, inner.x(), inner.bottom() - 1,
                    notification(state), notificationStyle(state, theme), inner);
            return new ContentLayout(List.of(), visibleRows);
        }

        WorkbenchSnapshot snapshot = state.snapshot().orElseThrow();
        List<RowHit> rowHits;
        if (state.selectedTab() == WorkbenchState.WorkbenchTab.OVERVIEW) {
            renderOverview(frame, inner, snapshot, theme);
            rowHits = List.of();
        } else {
            List<WorkbenchArtifactRow> rows = rows(snapshot, state.selectedTab());
            WorkbenchTableState table = table(state, state.selectedTab());
            boolean pipelineDetail = state.selectedTab() == WorkbenchState.WorkbenchTab.PIPELINES
                    && selectedPipelineStatus(state).isPresent();
            Rect tableArea = pipelineDetail
                    ? new Rect(inner.x(), inner.y(), inner.width(), Math.max(1, inner.height() - 2))
                    : inner;
            int tableRows = Math.max(1, tableArea.height() - 1);
            visibleRows = tableRows;
            rowHits = renderTable(
                    frame, tableArea, state.selectedTab(), rows, table, wide, tableRows, theme);
            if (pipelineDetail) {
                write(frame, inner.x(), inner.bottom() - 2,
                        pipelineStatusDetail(selectedPipelineStatus(state).orElseThrow()),
                        pipelineStatusStyle(selectedPipelineStatus(state).orElseThrow(), theme), inner);
            }
        }
        write(frame, inner.x(), inner.bottom() - 1,
                notification(state), notificationStyle(state, theme), inner);
        return new ContentLayout(rowHits, visibleRows);
    }

    private static void renderLogs(Frame frame, Rect area, Optional<WorkbenchLogsState> logs, WorkbenchTheme theme) {
        if (logs.isEmpty()) {
            Block block = panel("Logs", false, theme);
            frame.renderWidget(block, area);
            write(frame, block.inner(area).x(), block.inner(area).y(), "Select a remote Pipeline in 4 Pipelines.", theme.muted(), block.inner(area));
            return;
        }
        WorkbenchLogsState view = logs.orElseThrow();
        Block block = panel("[" + view.pipelineId() + "] Logs", false, theme);
        frame.renderWidget(block, area);
        Rect inner = block.inner(area);
        int y = inner.y();
        if (view.loading()) write(frame, inner.x(), y++, "Loading logs for " + view.pipelineId() + "...", theme.muted(), inner);
        if (view.truncated()) write(frame, inner.x(), y++, "Earlier logs are no longer retained on this node.", theme.warning(), inner);
        if (view.error().isPresent()) write(frame, inner.x(), y++, view.error().orElseThrow(), theme.error(), inner);
        if (view.lines().isEmpty() && !view.loading()) write(frame, inner.x(), y, "No logs received yet.", theme.muted(), inner);
        int visibleHeight = Math.max(1, inner.bottom() - y);
        List<Line> allLines = logLines(view.lines(), theme);
        int start = Math.max(0, allLines.size() - visibleHeight - view.scrollOffset());
        int end = Math.min(allLines.size(), start + visibleHeight);
        List<Line> visibleLines = new ArrayList<>(Math.max(0, end - start));
        for (int i = start; i < end; i++) visibleLines.add(allLines.get(i));
        Rect contentArea = inner;
        if (allLines.size() > visibleHeight) {
            List<Rect> chunks = Layout.horizontal()
                    .constraints(Constraint.fill(), Constraint.length(1))
                    .split(inner);
            ScrollbarState scrollState = new ScrollbarState();
            scrollState.contentLength(allLines.size())
                    .viewportContentLength(visibleHeight)
                    .position(start);
            frame.renderStatefulWidget(Scrollbar.builder().build(), chunks.get(1), scrollState);
            contentArea = chunks.getFirst();
        }
        if (!visibleLines.isEmpty()) {
            frame.renderWidget(Paragraph.builder()
                    .text(Text.from(visibleLines))
                    .overflow(view.wrapped() ? Overflow.WRAP_WORD : Overflow.CLIP)
                    .build(), new Rect(contentArea.x(), y, contentArea.width(), visibleHeight));
        }
        if (view.newLines()) write(frame, inner.right() - 3, inner.y(), "(*)", theme.accent(), inner);
    }

    private static void renderInspect(Frame frame, Rect area, WorkbenchState state, WorkbenchTheme theme) {
        Optional<WorkbenchInspectState> inspect = state.inspect();
        String title = inspect.map(value -> "[" + value.pipelineId() + "] Inspect").orElse("Inspect");
        Block block = panel(title, false, theme);
        frame.renderWidget(block, area);
        Rect inner = block.inner(area);
        if (inspect.isEmpty()) {
            write(frame, inner.x(), inner.y(), "Select a remote Pipeline in 4 Pipelines, then press 6.", theme.muted(), inner);
            return;
        }
        WorkbenchInspectState value = inspect.orElseThrow();
        int y = inner.y();
        switch (value) {
            case WorkbenchInspectState.Loading loading ->
                    write(frame, inner.x(), y, "Reading current metrics for " + loading.pipelineId() + "...", theme.muted(), inner);
            case WorkbenchInspectState.Rejected rejected -> {
                write(frame, inner.x(), y, "Metrics unavailable: " + rejected.code(), theme.error(), inner);
                write(frame, inner.x(), y + 1, rejected.message(), theme.muted(), inner);
            }
            case WorkbenchInspectState.Unreachable ignored ->
                    write(frame, inner.x(), y, "Metrics server is unreachable.", theme.error(), inner);
            case WorkbenchInspectState.Unavailable ignored ->
                    write(frame, inner.x(), y, "Sign in to read current Pipeline metrics.", theme.warning(), inner);
            case WorkbenchInspectState.Available available -> {
                write(frame, inner.x(), y++, "State", theme.title(), inner);
                Optional<WorkbenchPipelineStatus> status = selectedPipelineStatus(state);
                write(frame, inner.x() + 2, y++, status.map(WorkbenchRenderer::pipelineStatusDetail)
                        .orElse("Reading current state..."), status.map(statusValue -> pipelineStatusStyle(statusValue, theme))
                        .orElse(theme.muted()), inner);
                MovementReading current = available.current();
                String moving = current == null
                        ? "not published"
                        : MovementReading.describe(current.since(available.previous()));
                write(frame, inner.x(), y++, "Moving", theme.title(), inner);
                write(frame, inner.x() + 2, y++, moving, theme.base(), inner);
                write(frame, inner.x(), y++, "Lag", theme.title(), inner);
                write(frame, inner.x() + 2, y++, current == null ? "not published" : current.describeLag(), theme.base(), inner);
                if (!available.positionsNotCollected().isEmpty()) {
                    write(frame, inner.x(), y++, "Positions not collected: "
                            + String.join(", ", available.positionsNotCollected()), theme.warning(), inner);
                }
                if (!available.targetAckedPosition().isEmpty() && y < inner.bottom()) {
                    write(frame, inner.x(), y++, "Target-acked positions", theme.title(), inner);
                    for (Map.Entry<String, String> entry : available.targetAckedPosition().entrySet().stream()
                            .sorted(Map.Entry.comparingByKey()).toList()) {
                        if (y >= inner.bottom()) return;
                        write(frame, inner.x() + 2, y++, entry.getKey() + ": " + entry.getValue(), theme.base(), inner);
                    }
                }
                write(frame, inner.x(), y++, "Counters", theme.title(), inner);
                if (available.metrics().isEmpty()) {
                    write(frame, inner.x() + 2, y++, "No numeric counters published.", theme.muted(), inner);
                } else {
                    for (Map.Entry<String, Long> entry : available.metrics().entrySet().stream()
                            .sorted(Map.Entry.comparingByKey()).toList()) {
                        if (y >= inner.bottom()) return;
                        write(frame, inner.x() + 2, y++, entry.getKey() + ": " + entry.getValue(), theme.base(), inner);
                    }
                }
                if (!available.facts().isEmpty() && y < inner.bottom()) {
                    write(frame, inner.x(), y++, "Facts", theme.title(), inner);
                    for (MetricsOutcome.FactPoint fact : available.facts().stream()
                            .sorted(Comparator.comparing(MetricsOutcome.FactPoint::name)
                                    .thenComparing(fact -> fact.attributes().toString()))
                            .toList()) {
                        if (y >= inner.bottom()) return;
                        String attributes = fact.attributes().isEmpty() ? "" : " " + fact.attributes();
                        String observedAt = fact.observedAt() == null ? "" : " @ " + LOG_TIMESTAMP.format(fact.observedAt());
                        write(frame, inner.x() + 2, y++, fact.name() + attributes + ": " + fact.value() + observedAt,
                                theme.base(), inner);
                    }
                }
            }
        }
    }

    private static List<Line> logLines(List<RemoteLogLine> entries, WorkbenchTheme theme) {
        List<Line> lines = new ArrayList<>();
        for (RemoteLogLine entry : entries) {
            String[] messageLines = safeText(entry.message()).split("\\R", -1);
            lines.add(logLine(entry, messageLines.length == 0 ? "" : messageLines[0], theme));
            for (int index = 1; index < messageLines.length; index++) {
                List<Span> spans = new ArrayList<>();
                spans.add(Span.styled("                          ", theme.muted()));
                spans.addAll(logMessageSpans(messageLines[index], theme));
                lines.add(Line.from(spans));
            }
        }
        return lines;
    }

    private static Line logLine(RemoteLogLine line, String message, WorkbenchTheme theme) {
        String level = line.level() == null ? "UNKNOWN" : line.level().toUpperCase(Locale.ROOT);
        Style levelStyle = switch (level) {
            case "ERROR", "FATAL" -> theme.error().bold();
            case "WARN", "WARNING" -> theme.warning().bold();
            case "INFO" -> theme.success().bold();
            case "DEBUG" -> theme.info().bold();
            default -> theme.muted();
        };
        List<Span> spans = new ArrayList<>();
        spans.add(Span.styled(LOG_TIMESTAMP.format(Instant.ofEpochMilli(line.timestampMillis())) + "  ", theme.muted()));
        spans.add(Span.styled(String.format(Locale.ROOT, "%-5s", level) + " ", levelStyle));
        spans.addAll(logMessageSpans(message, theme));
        return Line.from(spans);
    }

    private static List<Span> logMessageSpans(String text, WorkbenchTheme theme) {
        List<Span> spans = new ArrayList<>();
        Matcher matcher = LOG_CLASS_OR_FRAME.matcher(text);
        int offset = 0;
        while (matcher.find()) {
            if (matcher.start() > offset) {
                spans.add(Span.styled(text.substring(offset, matcher.start()), theme.base()));
            }
            spans.add(Span.styled(matcher.group(), theme.codeKey()));
            offset = matcher.end();
        }
        if (offset < text.length() || spans.isEmpty()) {
            spans.add(Span.styled(text.substring(offset), theme.base()));
        }
        return spans;
    }

    private static ContentLayout renderWorkspace(
            Frame frame,
            Rect area,
            WorkbenchState state,
            WorkbenchSnapshot snapshot,
            WorkbenchTheme theme) {
        int leftWidth = Math.clamp(area.width() / 3, 28, 44);
        int infoHeight = Math.min(8, area.height() - 8);
        int filesHeight = area.height() - infoHeight;
        Rect filesArea = new Rect(area.x(), area.y(), leftWidth, filesHeight);
        Rect infoArea = new Rect(area.x(), area.y() + filesHeight, leftWidth, infoHeight);
        Rect viewerArea = new Rect(area.x() + leftWidth, area.y(), area.width() - leftWidth, area.height());

        boolean filesFocused = state.workspaceView().focus() == WorkbenchWorkspaceState.Focus.FILES
                && !state.workspaceView().editing();
        Block filesBlock = panel("Files", filesFocused, theme);
        Block infoBlock = panel("Info", filesFocused, theme);
        String viewerTitle = state.workspaceView().document()
                .map(document -> (document.editing() ? "Edit" : "YAML")
                        + " [" + fileName(document.relativePath())
                        + (document.dirty() ? " *" : "") + "]")
                .orElse("YAML");
        Block viewerBlock = panel(viewerTitle, !filesFocused, theme);
        frame.renderWidget(filesBlock, filesArea);
        frame.renderWidget(infoBlock, infoArea);
        frame.renderWidget(viewerBlock, viewerArea);

        Rect filesInner = filesBlock.inner(filesArea);
        List<WorkbenchArtifactRow> rows = sorted(snapshot.workspace().rows(), state.workspaceTable());
        int selected = rows.isEmpty()
                ? -1 : Math.clamp(state.workspaceTable().selectedIndex(), 0, rows.size() - 1);
        int capacity = Math.max(1, filesInner.height());
        int maximumScroll = Math.max(0, rows.size() - capacity);
        int scroll = Math.clamp(state.workspaceTable().scrollOffset(), 0, maximumScroll);
        if (selected >= 0 && selected < scroll) {
            scroll = selected;
        } else if (selected >= scroll + capacity) {
            scroll = selected - capacity + 1;
        }
        List<RowHit> hits = new ArrayList<>();
        int limit = Math.min(rows.size(), scroll + capacity);
        for (int index = scroll; index < limit; index++) {
            WorkbenchArtifactRow row = rows.get(index);
            String icon = switch (row.key().kind()) {
                case "source" -> "🔌";
                case "pipeline" -> "🔀";
                default -> "📄";
            };
            Path relativePath = row.local().getFirst().relativePath();
            boolean dirty = state.workspaceView().document()
                    .filter(document -> document.dirty()
                            && document.relativePath().equals(relativePath.normalize()))
                    .isPresent();
            String label = icon + " " + fileName(relativePath) + (dirty ? " *" : "");
            int y = filesInner.y() + index - scroll;
            Style style = index == selected ? theme.selection() : theme.base();
            int labelWidth = row.remote().isEmpty()
                    ? filesInner.width() : Math.max(1, filesInner.width() - 2);
            write(frame, filesInner.x(), y, pad(label, labelWidth), style, filesInner);
            if (!row.remote().isEmpty()) {
                write(frame, filesInner.right() - 1, y, "●", theme.success(), filesInner);
            }
            hits.add(new RowHit(
                    WorkbenchState.WorkbenchTab.WORKSPACE,
                    index,
                    new Rect(filesInner.x(), y, filesInner.width(), 1)));
        }
        if (rows.isEmpty()) {
            write(frame, filesInner.x(), filesInner.y(), "No workspace files.", theme.base(), filesInner);
        }

        Rect infoInner = infoBlock.inner(infoArea);
        if (selected >= 0) {
            WorkbenchArtifactRow row = rows.get(selected);
            int y = infoInner.y();
            y = renderInfoLine(frame, infoInner, y, "Kind", row.key().kind(), theme);
            y = renderInfoLine(frame, infoInner, y, "ID", row.key().id(), theme);
            y = renderInfoLine(frame, infoInner, y, "Path",
                    displayRelativePath(row.local().getFirst().relativePath()), theme);
            y = renderInfoLine(frame, infoInner, y, "Remote",
                    row.remote().isEmpty() ? "○ absent" : "● present", theme);
            renderInfoLine(frame, infoInner, y, "State", words(row.alignment()), theme);
        }

        Rect viewerInner = viewerBlock.inner(viewerArea);
        state.workspaceView().document().ifPresentOrElse(
                document -> renderWorkspaceDocument(frame, viewerInner, document, theme),
                () -> {
                    write(frame, viewerInner.x(), viewerInner.y(),
                            "Select a file and press Enter to open it.", theme.muted(), viewerInner);
                    writeNotification(frame, viewerInner.x(), viewerInner.bottom() - 1,
                            state, theme, viewerInner);
                });
        return new ContentLayout(List.copyOf(hits), capacity);
    }

    private static void renderWorkspaceDocument(
            Frame frame,
            Rect viewerInner,
            WorkbenchWorkspaceState.Document document,
            WorkbenchTheme theme) {
        boolean showQuickDoc = document.quickDoc().isPresent() && viewerInner.height() > 10;
        Rect documentArea = showQuickDoc
                ? new Rect(viewerInner.x(), viewerInner.y(), viewerInner.width(), viewerInner.height() - 4)
                : viewerInner;
        renderDocument(frame, documentArea, document, theme);
        if (showQuickDoc) {
            Rect quickDocArea = new Rect(
                    viewerInner.x(), viewerInner.bottom() - 4, viewerInner.width(), 4);
            renderQuickDoc(frame, quickDocArea, document.quickDoc().orElseThrow(), theme);
        }
    }

    private static void renderQuickDoc(
            Frame frame,
            Rect area,
            WorkbenchQuickDocProvider.QuickDoc quickDoc,
            WorkbenchTheme theme) {
        Style divider = quickDoc.validationError().isPresent() ? theme.error() : theme.muted();
        String prefix = "─── ";
        int titleWidth = displayWidth(quickDoc.title());
        int remaining = Math.max(0, area.width() - displayWidth(prefix) - titleWidth - 1);
        int x = area.x();
        x += write(frame, x, area.y(), prefix, divider, area);
        x += write(frame, x, area.y(), quickDoc.title(), divider.bold(), area);
        write(frame, x, area.y(), " " + "─".repeat(remaining), divider, area);
        for (int index = 0; index < quickDoc.entries().size() && index < area.height() - 1; index++) {
            write(frame, area.x(), area.y() + index + 1, quickDoc.entries().get(index), divider, area);
        }
    }

    private static Block panel(String title, boolean focused, WorkbenchTheme theme) {
        return Block.builder()
                .borderType(BorderType.ROUNDED)
                .borders(Borders.ALL)
                .borderStyle(focused ? theme.accent() : theme.base())
                .title(Title.from(Line.from(Span.styled(
                        " " + title + " ", focused ? theme.title() : theme.muted()))))
                .build();
    }

    private static int renderInfoLine(
            Frame frame, Rect area, int y, String label, String value, WorkbenchTheme theme) {
        if (displayWidth(label) + 2 + displayWidth(value) > area.width()) {
            write(frame, area.x(), y, label + ":", theme.muted(), area);
            write(frame, area.x() + 1, y + 1, value, theme.base(), area);
            return y + 2;
        }
        int x = area.x();
        x += write(frame, x, y, label + ": ", theme.muted(), area);
        write(frame, x, y, value, theme.base(), area);
        return y + 1;
    }

    private static void renderDocument(
            Frame frame,
            Rect area,
            WorkbenchWorkspaceState.Document document,
            WorkbenchTheme theme) {
        String[] lines = document.content().split("\\n", -1);
        int activeLine = document.editing() ? document.cursorLine() : document.selectedLine();
        int cursorColumn = document.cursorColumn();
        int scroll = Math.max(0, activeLine - area.height() + 1);
        int numberWidth = Integer.toString(lines.length).length();
        int limit = Math.min(lines.length, scroll + area.height());
        for (int index = scroll; index < limit; index++) {
            int y = area.y() + index - scroll;
            int x = area.x();
            boolean active = index == activeLine;
            Style rowBackground = active
                    ? theme.selection().bg()
                            .map(color -> Style.EMPTY.bg(color))
                            .orElse(Style.EMPTY)
                    : Style.EMPTY;
            if (active) {
                frame.buffer().setStyle(new Rect(area.x(), y, area.width(), 1), rowBackground);
            }
            if (document.editing()) {
                x += write(frame, x, y, pad(Integer.toString(index + 1), numberWidth) + " |",
                        theme.muted().patch(rowBackground), area);
                write(frame, x, y, lines[index], theme.base().patch(rowBackground), area);
            } else {
                x += write(frame, x, y, active ? ">> " : "   ",
                        active ? theme.label().bold().patch(rowBackground) : theme.base(), area);
                x += write(frame, x, y, pad(Integer.toString(index + 1), numberWidth) + " ",
                        (active ? theme.label().bold() : theme.muted()).patch(rowBackground), area);
                renderYamlLine(frame, x, y, lines[index], theme, rowBackground, area);
            }
            if (document.editing() && index == document.scopeLine() && index != activeLine) {
                frame.buffer().setStyle(new Rect(area.x(), y, area.width(), 1), theme.accent().bold());
            }
            if (document.editing() && index == activeLine && x < area.right()) {
                String cursor = cursorColumn < lines[index].length()
                        ? String.valueOf(lines[index].charAt(cursorColumn)) : " ";
                int cursorX = x + displayWidth(lines[index].substring(0, cursorColumn));
                write(frame, cursorX, y, cursor, theme.accentBackground(), area);
            }
        }
    }

    private static void renderYamlLine(
            Frame frame,
            int x,
            int y,
            String text,
            WorkbenchTheme theme,
            Style rowBackground,
            Rect area) {
        Style[] styles = new Style[text.length()];
        applyPattern(styles, text, YAML_COMMENT, theme.muted());

        Matcher key = YAML_KEY.matcher(text);
        if (key.find()) {
            applyRange(styles, key.start(2), key.end(2), theme.codeKey());
            int colon = text.indexOf(':', key.end(2));
            if (colon >= 0) {
                applyRange(styles, colon, colon + 1, theme.base().bold());
            }
        }
        applyPattern(styles, text, YAML_STRING_VALUE, theme.warning());
        applyPatternGroup(styles, text, YAML_BOOLEAN_NULL, 1, theme.info());
        applyPatternGroup(styles, text, YAML_NUMBER, 1, theme.info());

        Matcher listMarker = YAML_LIST_MARKER.matcher(text);
        if (listMarker.find()) {
            applyRange(styles, listMarker.start(2), listMarker.end(2), theme.base().bold());
        }
        int colon = text.indexOf(':');
        if (colon >= 0 && colon + 1 < text.length()) {
            int valueStart = colon + 1;
            while (valueStart < text.length() && text.charAt(valueStart) == ' ') {
                valueStart++;
            }
            boolean styledValue = false;
            for (int index = valueStart; index < text.length(); index++) {
                if (styles[index] != null) {
                    styledValue = true;
                    break;
                }
            }
            if (!styledValue) {
                applyRange(styles, valueStart, text.length(), theme.warning());
            }
        }

        int offset = 0;
        while (offset < text.length() && x < area.right()) {
            Style style = styles[offset] == null ? theme.base() : styles[offset];
            int end = offset + 1;
            while (end < text.length() && Objects.equals(styles[end], styles[offset])) {
                end++;
            }
            x += write(frame, x, y, text.substring(offset, end), style.patch(rowBackground), area);
            offset = end;
        }
    }

    private static void applyPattern(Style[] styles, String text, Pattern pattern, Style style) {
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {
            applyRange(styles, matcher.start(), matcher.end(), style);
        }
    }

    private static void applyPatternGroup(
            Style[] styles, String text, Pattern pattern, int group, Style style) {
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {
            applyRange(styles, matcher.start(group), matcher.end(group), style);
        }
    }

    private static void applyRange(Style[] styles, int start, int end, Style style) {
        for (int index = start; index < end && index < styles.length; index++) {
            if (styles[index] == null) {
                styles[index] = style;
            }
        }
    }

    private static void renderOpaquePopupSurface(Frame frame, Rect area, WorkbenchTheme theme) {
        frame.renderWidget(Clear.INSTANCE, area);
        frame.buffer().setStyle(area, theme.base());
    }

    private static void renderOverview(
            Frame frame, Rect area, WorkbenchSnapshot snapshot, WorkbenchTheme theme) {
        if (isFirstRun(snapshot)) {
            renderFirstRunOverview(frame, area, snapshot, theme);
            return;
        }
        WorkbenchOverviewSnapshot overview = snapshot.overview();
        int summaryY = area.bottom() - 4;
        int y = area.y();
        write(frame, area.x(), y++, "Workspace at a glance", theme.title(), area);
        write(frame, area.x(), y++, "Resources", theme.label().bold(), area);
        int rowsY = y;
        int kindCapacity = Math.max(0, summaryY - rowsY);
        boolean overflow = overview.kinds().size() > kindCapacity;
        int visibleKinds = Math.min(
                overview.kinds().size(),
                overflow ? Math.max(0, kindCapacity - 1) : kindCapacity);
        y = rowsY;
        for (int index = 0; index < visibleKinds; index++) {
            WorkbenchKindCount kind = overview.kinds().get(index);
            String remote = kind.remoteCount().isPresent()
                    ? Integer.toString(kind.remoteCount().orElseThrow())
                    : "unknown";
            write(frame, area.x(), y++,
                    pad(kind.kind(), 16) + "local " + kind.localCount() + "  remote " + remote,
                    theme.base(), area);
        }
        if (overflow) {
            write(frame, area.x(), y,
                    "+" + (overview.kinds().size() - visibleKinds) + " kinds not shown",
                    theme.muted(), area);
        }
        WorkbenchAlignmentCounts counts = overview.alignment();
        write(frame, area.x(), summaryY,
                "Alignment: local only " + counts.localOnly()
                        + " | remote only " + counts.remoteOnly()
                        + " | in sync " + counts.inSync(),
                theme.base(), area);
        write(frame, area.x(), summaryY + 1,
                "           drifted " + counts.drifted()
                        + " | invalid local " + counts.invalidLocal()
                        + " | unknown " + counts.unknown(),
                theme.base(), area);
    }

    private static boolean isFirstRun(WorkbenchSnapshot snapshot) {
        return snapshot.workspace().rows().isEmpty()
                && snapshot.overview().kinds().stream()
                        .allMatch(count -> count.localCount() == 0 && count.remoteCount().orElse(0) == 0)
                && isEmpty(snapshot.overview().alignment());
    }

    private static boolean isEmpty(WorkbenchAlignmentCounts counts) {
        return counts.localOnly() == 0
                && counts.remoteOnly() == 0
                && counts.inSync() == 0
                && counts.drifted() == 0
                && counts.invalidLocal() == 0
                && counts.unknown() == 0;
    }

    private static void renderFirstRunOverview(
            Frame frame, Rect area, WorkbenchSnapshot snapshot, WorkbenchTheme theme) {
        int x = area.x() + Math.min(4, Math.max(1, area.width() / 16));
        int y = area.y() + 1;
        write(frame, x + 4, y++, "╭────────╮       ╭──────────╮", theme.accent(), area);
        write(frame, x + 4, y++, "│ source │ ───▶  │ pipeline │", theme.accent(), area);
        write(frame, x + 4, y++, "╰────────╯       ╰──────────╯", theme.accent(), area);
        WorkbenchSessionSnapshot session = snapshot.session();
        String heading = switch (session.connection()) {
            case NO_CONTEXT -> "No workspace selected";
            case OFFLINE -> "No workspace artifacts found";
            case CONNECTED -> session.authentication() == WorkbenchAuthentication.SIGNED_OUT
                    ? "Sign in to continue"
                    : "No workspace artifacts found";
        };
        write(frame, x, y++, heading, theme.title(), area);
        y += 2;

        write(frame, x, y++, "💡 How to get started:", theme.label().bold(), area);
        if (session.connection() == WorkbenchConnection.NO_CONTEXT) {
            write(frame, x, y++, "🔌 Connect to a Tapstate Server:", theme.base().bold(), area);
            y = writeKeyInstruction(frame, area, x + 3, y, "c", "create or choose a context", theme);
            write(frame, x, y++, "🔐 Then sign in:", theme.base().bold(), area);
            y = writeKeyInstruction(frame, area, x + 3, y, "a", "open the sign-in form", theme);
        } else if (session.authentication() == WorkbenchAuthentication.SIGNED_OUT) {
            write(frame, x, y++, "🔐 Sign in to the selected context:", theme.base().bold(), area);
            y = writeKeyInstruction(frame, area, x + 3, y, "a", "open the sign-in form", theme);
        } else if (session.connection() == WorkbenchConnection.OFFLINE) {
            write(frame, x, y++, "🔌 Check the selected Tapstate Server:", theme.base().bold(), area);
            y = writeKeyInstruction(frame, area, x + 3, y, "c", "choose another context or edit this one", theme);
            y = writeKeyInstruction(frame, area, x + 3, y, "r", "refresh the connection", theme);
        }
        y++;
        write(frame, x, y++, "✨ Create a local source:", theme.base().bold(), area);
        y = writeKeyInstruction(frame, area, x + 3, y, "F2", "open Actions and choose New Source", theme);
        write(frame, x, y++, "🔀 Then create a pipeline from that source:", theme.base().bold(), area);
        y = writeKeyInstruction(frame, area, x + 3, y, "F2", "open Actions and choose New Pipeline", theme);
        y++;
        write(frame, x, y++, "🚀 Or create a demo workspace in another terminal:", theme.base().bold(), area);
        write(frame, x + 3, y, "> tapstate demo -w demo", theme.success(), area);
    }

    private static int writeKeyInstruction(
            Frame frame, Rect area, int x, int y, String key, String instruction, WorkbenchTheme theme) {
        int next = x + write(frame, x, y, "Press ", theme.base(), area);
        next += write(frame, next, y, " " + key + " ", theme.hintKey(), area);
        write(frame, next, y, " " + instruction, theme.base(), area);
        return y + 1;
    }

    private static List<RowHit> renderTable(
            Frame frame,
            Rect area,
            WorkbenchState.WorkbenchTab tab,
            List<WorkbenchArtifactRow> rows,
            WorkbenchTableState table,
            boolean wide,
            int visibleRows,
            WorkbenchTheme theme) {
        int markerWidth = 3;
        Rect tableArea = new Rect(
                area.x() + markerWidth,
                area.y(),
                Math.max(1, area.width() - markerWidth),
                area.height());
        Columns columns = Columns.forWidth(tableArea.width(), wide);
        renderTableHeader(frame, tableArea, columns, table, theme);
        if (rows.isEmpty()) {
            write(frame, area.x(), area.y() + 1,
                    emptyRowsMessage(tab), theme.base(), area);
            return List.of();
        }

        List<WorkbenchArtifactRow> sortedRows = sorted(rows, table);
        int selected = Math.clamp(table.selectedIndex(), 0, sortedRows.size() - 1);
        int maximumScroll = Math.max(0, rows.size() - visibleRows);
        int scroll = Math.clamp(table.scrollOffset(), 0, maximumScroll);
        if (selected < scroll) {
            scroll = selected;
        } else if (selected >= scroll + visibleRows) {
            scroll = selected - visibleRows + 1;
        }

        List<RowHit> hits = new ArrayList<>();
        int limit = Math.min(sortedRows.size(), scroll + visibleRows);
        for (int index = scroll; index < limit; index++) {
            WorkbenchArtifactRow row = sortedRows.get(index);
            String line = columns.format(
                    row.key().id(),
                    words(row.alignment()),
                    localMarker(row));
            int y = area.y() + 1 + index - scroll;
            Style style = index == selected ? theme.selection() : theme.base();
            String marker = index == selected ? ">> " : "   ";
            int width = write(frame, area.x(), y, marker + line, style, area);
            if (width > 0) {
                hits.add(new RowHit(tab, index, new Rect(area.x(), y, width, 1)));
            }
        }
        return List.copyOf(hits);
    }

    private static void renderTableHeader(
            Frame frame,
            Rect area,
            Columns columns,
            WorkbenchTableState table,
            WorkbenchTheme theme) {
        int x = area.x();
        WorkbenchSortColumn[] sortColumns = WorkbenchSortColumn.values();
        int[] widths = {columns.identifier(), columns.alignment(), columns.local()};
        for (int index = 0; index < sortColumns.length; index++) {
            WorkbenchSortColumn column = sortColumns[index];
            boolean active = column == table.sortColumn();
            String label = column.label() + (active ? table.sortReversed() ? "▲" : "▼" : "");
            x += write(frame, x, area.y(), pad(label, widths[index]),
                    active ? theme.label().bold() : theme.base().bold(), area);
            if (index < sortColumns.length - 1) {
                x += write(frame, x, area.y(), columns.separator(), theme.muted(), area);
            }
        }
    }

    static List<WorkbenchArtifactRow> sorted(
            List<WorkbenchArtifactRow> rows, WorkbenchTableState table) {
        Comparator<WorkbenchArtifactRow> comparator = switch (table.sortColumn()) {
            case IDENTIFIER -> Comparator.comparing(row -> row.key().id(), String.CASE_INSENSITIVE_ORDER);
            case ALIGNMENT -> Comparator.comparing(row -> words(row.alignment()), String.CASE_INSENSITIVE_ORDER);
            case LOCAL -> Comparator.comparing(WorkbenchRenderer::localMarker, String.CASE_INSENSITIVE_ORDER);
        };
        comparator = comparator
                .thenComparing(row -> row.key().kind(), String.CASE_INSENSITIVE_ORDER)
                .thenComparing(row -> row.key().id(), String.CASE_INSENSITIVE_ORDER);
        if (table.sortReversed()) {
            comparator = comparator.reversed();
        }
        return rows.stream().sorted(comparator).toList();
    }

    private static String notification(WorkbenchState state) {
        if (state.snapshot().isEmpty()) {
            return state.expectedSnapshot().isPresent()
                    ? "Loading workbench snapshot..."
                    : "No snapshot loaded. Press r to refresh.";
        }
        WorkbenchSnapshot snapshot = state.snapshot().orElseThrow();
        if (state.expectedSnapshot()
                .map(WorkbenchSnapshot::identity)
                .filter(snapshot.identity()::equals)
                .isEmpty()) {
            return "Refreshing workbench snapshot...";
        }
        WorkbenchRemoteState remote = remoteState(snapshot, state.selectedTab());
        return switch (remote) {
            case WorkbenchRemoteState.Available available -> available.artifactCount() == 0
                    ? "Remote workspace is empty"
                    : "";
            case WorkbenchRemoteState.NotConfigured ignored -> "No context selected";
            case WorkbenchRemoteState.SignedOut ignored -> "Signed out";
            case WorkbenchRemoteState.Offline ignored -> "Server offline";
            case WorkbenchRemoteState.Rejected rejected ->
                    "Remote rejected: " + rejected.code() + ", " + rejected.message();
            case WorkbenchRemoteState.Diagnostic diagnostic ->
                    "Diagnostic: " + diagnostic.code().code();
        };
    }

    private static FooterLayout renderFooter(
            Frame frame, Rect area, int y, WorkbenchState state, WorkbenchTheme theme) {
        boolean hasSelectableRows = state.selectedTab().hasTable()
                && state.snapshot()
                        .map(snapshot -> !rows(snapshot, state.selectedTab()).isEmpty())
                        .orElse(false);
        List<FooterHint> hints;
        if (state.overlay().isPresent()) {
            hints = overlayFooter(state.overlay().orElseThrow());
        } else {
            hints = switch (state.selectedTab()) {
            case OVERVIEW -> List.of(
                    new FooterHint("c", "context", Optional.of(FooterAction.CONTEXT)),
                    new FooterHint("a", "auth", Optional.of(FooterAction.AUTH)),
                    new FooterHint("0", "more", Optional.of(FooterAction.MORE)),
                    new FooterHint("F6", "shell", Optional.of(FooterAction.SHELL)),
                    new FooterHint("r", "refresh", Optional.of(FooterAction.REFRESH)),
                    new FooterHint("q", "quit", Optional.of(FooterAction.QUIT)));
            case WORKSPACE -> hasSelectableRows
                    ? workspaceFooter(state)
                    : List.of(
                            new FooterHint("Esc", "back", Optional.of(FooterAction.BACK)),
                            new FooterHint("F6", "shell", Optional.of(FooterAction.SHELL)),
                            new FooterHint("r", "refresh", Optional.of(FooterAction.REFRESH)),
                            new FooterHint("q", "quit", Optional.of(FooterAction.QUIT)));
            case SOURCES -> hasSelectableRows
                    ? List.of(
                            new FooterHint("↑↓", "navigate", Optional.empty()),
                            new FooterHint("Esc", "back", Optional.of(FooterAction.BACK)),
                            new FooterHint("s", "sort", Optional.of(FooterAction.SORT)),
                            new FooterHint("F6", "shell", Optional.of(FooterAction.SHELL)),
                            new FooterHint("r", "refresh", Optional.of(FooterAction.REFRESH)),
                            new FooterHint("q", "quit", Optional.of(FooterAction.QUIT)))
                    : List.of(
                            new FooterHint("Esc", "back", Optional.of(FooterAction.BACK)),
                            new FooterHint("F6", "shell", Optional.of(FooterAction.SHELL)),
                            new FooterHint("r", "refresh", Optional.of(FooterAction.REFRESH)),
                            new FooterHint("q", "quit", Optional.of(FooterAction.QUIT)));
            case PIPELINES -> pipelineFooter(state, hasSelectableRows);
            case LOGS -> List.of(new FooterHint("↑↓", "scroll", Optional.empty()), new FooterHint("Home/End", "top/live", Optional.empty()), new FooterHint("PgUp/PgDn", "page", Optional.empty()),
                    new FooterHint("l", "level", Optional.empty()),
                    new FooterHint("f", "follow " + state.logs().map(value -> value.following() ? "[on]" : "[off]").orElse("[on]"), Optional.empty()),
                    new FooterHint("w", "wrap " + state.logs().map(value -> value.wrapped() ? "[on]" : "[off]").orElse("[on]"), Optional.empty()),
                    new FooterHint("Esc", "back", Optional.of(FooterAction.BACK)), new FooterHint("q", "quit", Optional.of(FooterAction.QUIT)));
            case INSPECT -> List.of(
                    new FooterHint("r", "refresh", Optional.of(FooterAction.REFRESH)),
                    new FooterHint("Esc", "back", Optional.of(FooterAction.BACK)),
                    new FooterHint("q", "quit", Optional.of(FooterAction.QUIT)));
            };
        }
        if (state.overlay().isEmpty()) {
            List<FooterHint> rootHints = new ArrayList<>(hints.size() + 1);
            rootHints.add(new FooterHint("F2", "actions", Optional.of(FooterAction.ACTIONS)));
            rootHints.addAll(hints);
            hints = List.copyOf(rootHints);
        }
        hints = compactFooterHints(hints, area.width());
        int x = area.x();
        List<FooterHit> hits = new ArrayList<>();
        for (FooterHint hint : hints) {
            int start = x;
            x += write(frame, x, y, " " + hint.key() + " ", theme.hintKey(), area);
            x += write(frame, x, y, " " + hint.label() + "   ", theme.base(), area);
            int end = x;
            hint.action().ifPresent(action -> {
                if (end > start) {
                    hits.add(new FooterHit(action, new Rect(start, y, end - start, 1)));
                }
            });
            if (x >= area.right()) {
                return new FooterLayout(hits);
            }
        }
        return new FooterLayout(hits);
    }

    private static List<FooterHint> compactFooterHints(List<FooterHint> hints, int width) {
        if (footerWidth(hints) <= width) {
            return hints;
        }
        List<FooterHint> compacted = new ArrayList<>(hints);
        compacted.removeIf(hint -> "F6".equals(hint.key()) && "shell".equals(hint.label()));
        return List.copyOf(compacted);
    }

    private static int footerWidth(List<FooterHint> hints) {
        return hints.stream().mapToInt(hint -> hint.key().length() + hint.label().length() + 7).sum();
    }

    private static List<FooterHint> pipelineFooter(WorkbenchState state, boolean hasSelectableRows) {
        if (!hasSelectableRows) {
            return List.of(new FooterHint("Esc", "back", Optional.of(FooterAction.BACK)),
                    new FooterHint("F6", "shell", Optional.of(FooterAction.SHELL)),
                    new FooterHint("r", "refresh", Optional.of(FooterAction.REFRESH)),
                    new FooterHint("q", "quit", Optional.of(FooterAction.QUIT)));
        }
        List<FooterHint> hints = new ArrayList<>(List.of(
                new FooterHint("↑↓", "navigate", Optional.empty()),
                new FooterHint("Esc", "back", Optional.of(FooterAction.BACK)),
                new FooterHint("s", "sort", Optional.of(FooterAction.SORT))));
        selectedPipeline(state).ifPresent(row -> {
            boolean remoteAvailable = state.snapshot().orElseThrow().pipelines().remoteState()
                    instanceof WorkbenchRemoteState.Available;
            if (row.local().size() == 1 && row.local().getFirst().valid() && remoteAvailable) {
                hints.add(new FooterHint("F10", "apply", Optional.of(FooterAction.APPLY_PIPELINE)));
            }
            if (!row.remote().isEmpty() && remoteAvailable) {
                hints.add(new FooterHint("5", "logs", Optional.of(FooterAction.LOGS)));
                hints.add(new FooterHint("6", "inspect", Optional.of(FooterAction.INSPECT)));
                hints.add(new FooterHint("F5", "start", Optional.of(FooterAction.START_PIPELINE)));
                hints.add(new FooterHint("p", "pause", Optional.of(FooterAction.PAUSE_PIPELINE)));
                hints.add(new FooterHint("u", "resume", Optional.of(FooterAction.RESUME_PIPELINE)));
                hints.add(new FooterHint("x", "stop", Optional.of(FooterAction.STOP_PIPELINE)));
            }
        });
        hints.addAll(List.of(new FooterHint("F6", "shell", Optional.of(FooterAction.SHELL)),
                new FooterHint("r", "refresh", Optional.of(FooterAction.REFRESH)),
                new FooterHint("q", "quit", Optional.of(FooterAction.QUIT))));
        return List.copyOf(hints);
    }

    private static Optional<WorkbenchArtifactRow> selectedPipeline(WorkbenchState state) {
        if (state.snapshot().isEmpty()) {
            return Optional.empty();
        }
        List<WorkbenchArtifactRow> rows = sorted(state.snapshot().orElseThrow().pipelines().rows(), state.pipelinesTable());
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        WorkbenchArtifactRow row = rows.get(Math.clamp(state.pipelinesTable().selectedIndex(), 0, rows.size() - 1));
        return "pipeline".equals(row.key().kind()) ? Optional.of(row) : Optional.empty();
    }

    private static Optional<WorkbenchPipelineStatus> selectedPipelineStatus(WorkbenchState state) {
        return selectedPipeline(state).flatMap(row -> state.selectedPipelineStatus()
                .filter(status -> status.pipelineId().equals(row.key().id())));
    }

    private static String pipelineStatusDetail(WorkbenchPipelineStatus status) {
        return switch (status) {
            case WorkbenchPipelineStatus.Loading ignored -> "Selected Pipeline status: loading...";
            case WorkbenchPipelineStatus.Available available -> "Selected Pipeline status: " + available.state()
                    + available.failureMessage().map(message -> " · " + message).orElse("");
            case WorkbenchPipelineStatus.Rejected rejected -> "Selected Pipeline status unavailable: "
                    + rejected.code() + " · " + rejected.message();
            case WorkbenchPipelineStatus.Unreachable ignored -> "Selected Pipeline status unavailable: server unreachable";
            case WorkbenchPipelineStatus.Unavailable ignored -> "Selected Pipeline status unavailable";
        };
    }

    private static Style pipelineStatusStyle(WorkbenchPipelineStatus status, WorkbenchTheme theme) {
        return switch (status) {
            case WorkbenchPipelineStatus.Available available -> available.failureCode().isPresent()
                    ? theme.error() : theme.success();
            case WorkbenchPipelineStatus.Rejected ignored -> theme.error();
            case WorkbenchPipelineStatus.Unreachable ignored -> theme.warning();
            case WorkbenchPipelineStatus.Unavailable ignored -> theme.muted();
            case WorkbenchPipelineStatus.Loading ignored -> theme.info();
        };
    }

    private static List<FooterHint> workspaceFooter(WorkbenchState state) {
        if (state.workspaceView().document()
                .map(WorkbenchWorkspaceState.Document::pendingDiscard)
                .orElse(false)) {
            return List.of(
                    new FooterHint("Enter", "confirm", Optional.of(FooterAction.DISCARD)),
                    new FooterHint("F6", "shell", Optional.of(FooterAction.SHELL)),
                    new FooterHint("Esc", "cancel", Optional.of(FooterAction.CANCEL_DISCARD)));
        }
        if (state.workspaceView().editing()) {
            return List.of(
                    new FooterHint("↑↓←→", "navigate", Optional.empty()),
                    new FooterHint("Esc", "cancel", Optional.of(FooterAction.CANCEL_EDIT)),
                    new FooterHint("Ctrl+S", "save", Optional.of(FooterAction.SAVE)),
                    new FooterHint("F6", "shell", Optional.of(FooterAction.SHELL)),
                    new FooterHint("F5", "save & close", Optional.of(FooterAction.SAVE_AND_CLOSE)));
        }
        if (state.workspaceView().focus() == WorkbenchWorkspaceState.Focus.VIEWER) {
            return List.of(
                    new FooterHint("↑↓", "navigate", Optional.empty()),
                    new FooterHint("Esc", "back", Optional.of(FooterAction.BACK)),
                    new FooterHint("F4", "edit", Optional.of(FooterAction.EDIT)),
                    new FooterHint("F6", "shell", Optional.of(FooterAction.SHELL)),
                    new FooterHint("Tab", "files", Optional.of(FooterAction.TOGGLE_FOCUS)));
        }
        List<FooterHint> hints = new ArrayList<>(List.of(
                new FooterHint("↑↓", "navigate", Optional.empty()),
                new FooterHint("Esc", "back", Optional.of(FooterAction.BACK)),
                new FooterHint("Enter", "open", Optional.of(FooterAction.OPEN)),
                new FooterHint("F4", "edit", Optional.of(FooterAction.EDIT)),
                new FooterHint("F6", "shell", Optional.of(FooterAction.SHELL))));
        selectedWorkspaceArtifact(state).ifPresent(row -> {
            boolean remoteAvailable = state.snapshot().orElseThrow().workspace().remoteState()
                    instanceof WorkbenchRemoteState.Available;
            boolean applicable = ("source".equals(row.key().kind()) || "pipeline".equals(row.key().kind()))
                    && row.local().size() == 1 && row.local().getFirst().valid();
            if (remoteAvailable && applicable) {
                hints.add(4, new FooterHint("F10", "apply", Optional.of(FooterAction.APPLY_SELECTED_ARTIFACT)));
            }
        });
        if (state.workspaceView().document().isPresent()) {
            hints.add(new FooterHint("Tab", "viewer", Optional.of(FooterAction.TOGGLE_FOCUS)));
        }
        return List.copyOf(hints);
    }

    private static Optional<WorkbenchArtifactRow> selectedWorkspaceArtifact(WorkbenchState state) {
        if (state.snapshot().isEmpty()) {
            return Optional.empty();
        }
        List<WorkbenchArtifactRow> rows = sorted(state.snapshot().orElseThrow().workspace().rows(), state.workspaceTable());
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(rows.get(Math.clamp(state.workspaceTable().selectedIndex(), 0, rows.size() - 1)));
    }

    private static List<FooterHint> sourceCreateFooter(WorkbenchOverlayState.SourceCreate source) {
        List<FooterHint> hints = new ArrayList<>();
        if (source.stage() == WorkbenchOverlayState.SourceCreate.Stage.CONFIG) {
            hints.add(new FooterHint("↑↓", "fields", Optional.empty()));
            hints.add(new FooterHint("←→", "choices", Optional.empty()));
            hints.add(new FooterHint("type", "value", Optional.empty()));
        } else {
            hints.add(new FooterHint(source.stage() == WorkbenchOverlayState.SourceCreate.Stage.CONNECTOR
                    || source.stage() == WorkbenchOverlayState.SourceCreate.Stage.MODE ? "↑↓" : "type",
                    source.stage() == WorkbenchOverlayState.SourceCreate.Stage.PREVIEW ? "preview" : "choose",
                    Optional.empty()));
        }
        if (source.stage() == WorkbenchOverlayState.SourceCreate.Stage.PREVIEW) {
            hints.add(new FooterHint("F4", "edit", Optional.of(FooterAction.EDIT)));
        }
        hints.add(new FooterHint("Enter", source.pending() ? "wait"
                : source.stage() == WorkbenchOverlayState.SourceCreate.Stage.PREVIEW ? "create" : "next",
                Optional.empty()));
        hints.add(new FooterHint("Esc", source.stage() == WorkbenchOverlayState.SourceCreate.Stage.PREVIEW
                ? "back" : "cancel", Optional.empty()));
        return List.copyOf(hints);
    }

    private static List<FooterHint> overlayFooter(WorkbenchOverlayState overlay) {
        return switch (overlay) {
            case WorkbenchOverlayState.More ignored -> List.of(
                    new FooterHint("↑↓", "navigate", Optional.empty()),
                    new FooterHint("Enter", "select", Optional.empty()),
                    new FooterHint("Esc", "back", Optional.empty()));
            case WorkbenchOverlayState.ContextPicker ignored -> List.of(
                    new FooterHint("↑↓", "navigate", Optional.empty()),
                    new FooterHint("Enter", "select", Optional.empty()),
                    new FooterHint("d", "delete", Optional.empty()),
                    new FooterHint("Esc", "back", Optional.empty()));
            case WorkbenchOverlayState.ContextCreate create -> List.of(
                    new FooterHint("↑↓", "fields", Optional.empty()),
                    new FooterHint("Enter", create.pending() ? "wait" : "next", Optional.empty()),
                    new FooterHint("Esc", "cancel", Optional.empty()));
            case WorkbenchOverlayState.SourceCreate source -> sourceCreateFooter(source);
            case WorkbenchOverlayState.SourceYamlEditor editor -> List.of(
                    new FooterHint("↑↓←→", "navigate", Optional.empty()),
                    new FooterHint("Esc", "cancel", Optional.empty()),
                    new FooterHint("F5", "save & close", Optional.of(FooterAction.SAVE_AND_CLOSE)));
            case WorkbenchOverlayState.PipelineCreate pipeline -> pipelineCreateFooter(pipeline);
            case WorkbenchOverlayState.PipelineYamlEditor editor -> List.of(
                    new FooterHint("↑↓←→", "navigate", Optional.empty()),
                    new FooterHint("Esc", "cancel", Optional.empty()),
                    new FooterHint("F5", "save & close", Optional.of(FooterAction.SAVE_AND_CLOSE)));
            case WorkbenchOverlayState.Confirm confirm -> confirm.pending()
                    ? List.of(new FooterHint("…", "working", Optional.empty()))
                    : List.of(
                            new FooterHint("Enter", "confirm", Optional.of(FooterAction.CONFIRM)),
                            new FooterHint("Esc", "cancel", Optional.of(FooterAction.CANCEL_CONFIRM)));
            case WorkbenchOverlayState.Login login -> List.of(
                    new FooterHint("↑↓", "fields", Optional.empty()),
                    new FooterHint("Enter", login.pending() ? "wait" : "next", Optional.empty()),
                    new FooterHint("Esc", "cancel", Optional.empty()));
            case WorkbenchOverlayState.Actions ignored -> List.of(
                    new FooterHint("↑↓", "navigate", Optional.empty()),
                    new FooterHint("Enter", "run", Optional.empty()),
                    new FooterHint("Esc", "back", Optional.empty()));
            case WorkbenchOverlayState.LogLevel ignored -> List.of(
                    new FooterHint("↑↓", "navigate", Optional.empty()),
                    new FooterHint("Enter", "select", Optional.empty()),
                    new FooterHint("Esc", "back", Optional.empty()));
            case WorkbenchOverlayState.Help ignored -> List.of(
                    new FooterHint("Esc", "back", Optional.empty()));
        };
    }

    private static List<FooterHint> pipelineCreateFooter(WorkbenchOverlayState.PipelineCreate pipeline) {
        List<FooterHint> hints = new ArrayList<>();
        hints.add(new FooterHint(pipeline.stage() == WorkbenchOverlayState.PipelineCreate.Stage.SOURCE ? "↑↓" : "type",
                pipeline.stage() == WorkbenchOverlayState.PipelineCreate.Stage.PREVIEW ? "preview" : "choose",
                Optional.empty()));
        if (pipeline.stage() == WorkbenchOverlayState.PipelineCreate.Stage.PREVIEW) {
            hints.add(new FooterHint("F4", "edit", Optional.of(FooterAction.EDIT)));
        }
        hints.add(new FooterHint("Enter", pipeline.pending() ? "wait"
                : pipeline.stage() == WorkbenchOverlayState.PipelineCreate.Stage.PREVIEW ? "create" : "next",
                Optional.empty()));
        hints.add(new FooterHint("Esc", pipeline.stage() == WorkbenchOverlayState.PipelineCreate.Stage.PREVIEW
                ? "back" : "cancel", Optional.empty()));
        return List.copyOf(hints);
    }

    private static String tabLabel(WorkbenchState.WorkbenchTab tab) {
        return tabIcon(tab) + "  " + tabNumber(tab) + " " + tabName(tab);
    }

    private static String tabIcon(WorkbenchState.WorkbenchTab tab) {
        return switch (tab) {
            case OVERVIEW -> "🌊";
            case WORKSPACE -> "💻";
            case SOURCES -> "🔌";
            case PIPELINES -> "🔀";
            case LOGS -> "📜";
            case INSPECT -> "🔎";
        };
    }

    private static String tabNumber(WorkbenchState.WorkbenchTab tab) {
        return switch (tab) {
            case OVERVIEW -> "1";
            case WORKSPACE -> "2";
            case SOURCES -> "3";
            case PIPELINES -> "4";
            case LOGS -> "5";
            case INSPECT -> "6";
        };
    }

    private static String tabName(WorkbenchState.WorkbenchTab tab) {
        return switch (tab) {
            case OVERVIEW -> "Overview";
            case WORKSPACE -> "Workspace";
            case SOURCES -> "Sources";
            case PIPELINES -> "Pipelines";
            case LOGS -> "Logs";
            case INSPECT -> "Inspect";
        };
    }

    private static String tabBadge(WorkbenchState state, WorkbenchState.WorkbenchTab tab) {
        if (tab == WorkbenchState.WorkbenchTab.OVERVIEW || tab == WorkbenchState.WorkbenchTab.LOGS
                || tab == WorkbenchState.WorkbenchTab.INSPECT) {
            return "";
        }
        String count = state.snapshot().map(snapshot -> switch (tab) {
            case OVERVIEW -> "";
            case WORKSPACE -> Integer.toString(snapshot.workspace().rows().size());
            case SOURCES -> snapshot.sources().remoteState() instanceof WorkbenchRemoteState.Available
                    ? Integer.toString(snapshot.sources().rows().size())
                    : "?";
            case PIPELINES -> snapshot.pipelines().remoteState() instanceof WorkbenchRemoteState.Available
                    ? Integer.toString(snapshot.pipelines().rows().size())
                    : "?";
            case LOGS -> "";
            case INSPECT -> "";
        }).orElse("?");
        return "(" + count + ")";
    }

    private static Style notificationStyle(WorkbenchState state, WorkbenchTheme theme) {
        if (state.snapshot().isEmpty() || state.expectedSnapshot().isEmpty()) {
            return theme.muted();
        }
        WorkbenchRemoteState remote = remoteState(state.snapshot().orElseThrow(), state.selectedTab());
        return switch (remote) {
            case WorkbenchRemoteState.Available ignored -> theme.success();
            case WorkbenchRemoteState.NotConfigured ignored -> theme.warning();
            case WorkbenchRemoteState.SignedOut ignored -> theme.warning();
            case WorkbenchRemoteState.Offline ignored -> theme.warning();
            case WorkbenchRemoteState.Rejected ignored -> theme.error();
            case WorkbenchRemoteState.Diagnostic ignored -> theme.error();
        };
    }

    private static WorkbenchRemoteState remoteState(
            WorkbenchSnapshot snapshot, WorkbenchState.WorkbenchTab tab) {
        return switch (tab) {
            case OVERVIEW, WORKSPACE -> snapshot.workspace().remoteState();
            case SOURCES -> snapshot.sources().remoteState();
            case PIPELINES -> snapshot.pipelines().remoteState();
            case LOGS -> snapshot.pipelines().remoteState();
            case INSPECT -> snapshot.pipelines().remoteState();
        };
    }

    private static List<WorkbenchArtifactRow> rows(
            WorkbenchSnapshot snapshot, WorkbenchState.WorkbenchTab tab) {
        return switch (tab) {
            case OVERVIEW -> List.of();
            case WORKSPACE -> snapshot.workspace().rows();
            case SOURCES -> snapshot.sources().rows();
            case PIPELINES -> snapshot.pipelines().rows();
            case LOGS -> List.of();
            case INSPECT -> List.of();
        };
    }

    private static WorkbenchTableState table(
            WorkbenchState state, WorkbenchState.WorkbenchTab tab) {
        return switch (tab) {
            case OVERVIEW -> WorkbenchTableState.empty();
            case WORKSPACE -> state.workspaceTable();
            case SOURCES -> state.sourcesTable();
            case PIPELINES -> state.pipelinesTable();
            case LOGS -> WorkbenchTableState.empty();
            case INSPECT -> WorkbenchTableState.empty();
        };
    }

    private static String localMarker(WorkbenchArtifactRow row) {
        if (row.local().isEmpty()) {
            return "-";
        }
        String first = displayRelativePath(row.local().getFirst().relativePath());
        return row.local().size() == 1
                ? first
                : first + " (+" + (row.local().size() - 1) + ')';
    }

    private static String emptyRowsMessage(WorkbenchState.WorkbenchTab tab) {
        return switch (tab) {
            case OVERVIEW -> "No overview data.";
            case WORKSPACE -> "No workspace artifacts.";
            case SOURCES -> "No sources.";
            case PIPELINES -> "No pipelines.";
            case LOGS -> "No logs.";
            case INSPECT -> "No current Pipeline metrics.";
        };
    }

    private static String connectionMarker(WorkbenchConnection connection) {
        return connection == WorkbenchConnection.CONNECTED ? "●" : "○";
    }

    private static Style connectionStyle(WorkbenchConnection connection, WorkbenchTheme theme) {
        return connection == WorkbenchConnection.CONNECTED ? theme.success() : theme.warning();
    }

    private static String displayAuthentication(WorkbenchSessionSnapshot session) {
        return session.principal().orElseGet(() -> words(session.authentication()));
    }

    private static Style authenticationStyle(
            WorkbenchAuthentication authentication, WorkbenchTheme theme) {
        return switch (authentication) {
            case SIGNED_IN, MACHINE -> theme.success();
            case SIGNED_OUT -> theme.warning();
            case NOT_APPLICABLE -> theme.muted();
        };
    }

    private static String displayPath(Path path) {
        return path.normalize().toString();
    }

    private static String displayRelativePath(Path path) {
        Path normalized = path.normalize();
        if (normalized.isAbsolute() || normalized.startsWith("..")) {
            Path fileName = normalized.getFileName();
            return fileName == null ? "-" : fileName.toString();
        }
        return normalized.toString();
    }

    private static String fileName(Path path) {
        Path name = path.normalize().getFileName();
        return name == null ? "-" : name.toString();
    }

    private static void writeNotification(
            Frame frame, int x, int y, WorkbenchState state, WorkbenchTheme theme, Rect area) {
        String notification = notification(state);
        if (!notification.isBlank()) {
            write(frame, x, y, notification, notificationStyle(state, theme), area);
        }
    }

    private static String words(Enum<?> value) {
        return value.name().toLowerCase(Locale.ROOT).replace('_', ' ');
    }

    private static void writeCentered(Frame frame, Rect area, int y, String text, Style style) {
        int textWidth = displayWidth(text);
        int x = area.x() + Math.max(0, (area.width() - textWidth) / 2);
        write(frame, x, y, text, style, area);
    }

    private static int write(Frame frame, int x, int y, String text, Style style, Rect area) {
        if (y < area.top() || y >= area.bottom() || x < area.left() || x >= area.right()) {
            return 0;
        }
        String clipped = clip(text, area.right() - x);
        if (clipped.isEmpty()) {
            return 0;
        }
        int endX = frame.buffer().setString(x, y, clipped, style);
        return Math.min(area.right(), endX) - x;
    }

    private static String safeText(String value) {
        StringBuilder safe = new StringBuilder(value.length());
        for (int offset = 0; offset < value.length();) {
            int codePoint = value.codePointAt(offset);
            int type = Character.getType(codePoint);
            safe.appendCodePoint(Character.isISOControl(codePoint) || type == Character.FORMAT
                    ? ' '
                    : codePoint);
            offset += Character.charCount(codePoint);
        }
        return safe.toString();
    }

    private static String clip(String value, int maximumWidth) {
        if (maximumWidth <= 0) {
            return "";
        }
        String safeValue = safeText(value);
        return CharWidth.substringByWidth(safeValue, maximumWidth);
    }

    private static int displayWidth(String value) {
        return CharWidth.of(value);
    }

    private static String pad(String value, int width) {
        String clipped = clip(value, width);
        return clipped + " ".repeat(Math.max(0, width - displayWidth(clipped)));
    }

    record RenderLayout(
            boolean tooSmall,
            boolean wide,
            int visibleRowCapacity,
            List<TabHit> tabHits,
            List<RowHit> rowHits,
            List<ActionHit> actionHits,
            List<FooterHit> footerHits,
            List<OverlayHit> overlayHits) {

        RenderLayout {
            if (visibleRowCapacity < 0) {
                throw new IllegalArgumentException("Visible row capacity must not be negative");
            }
            tabHits = List.copyOf(tabHits);
            rowHits = List.copyOf(rowHits);
            actionHits = List.copyOf(actionHits);
            footerHits = List.copyOf(footerHits);
            overlayHits = List.copyOf(overlayHits);
        }

        static RenderLayout forTooSmallFrame() {
            return new RenderLayout(true, false, 0, List.of(), List.of(), List.of(), List.of(), List.of());
        }

        Optional<WorkbenchState.WorkbenchTab> tabAt(int x, int y) {
            return tabHits.stream()
                    .filter(hit -> hit.area().contains(x, y))
                    .map(TabHit::tab)
                    .findFirst();
        }

        Optional<RowHit> rowAt(int x, int y) {
            return rowHits.stream()
                    .filter(hit -> hit.area().contains(x, y))
                    .findFirst();
        }

        Optional<Launcher> actionAt(int x, int y) {
            return actionHits.stream()
                    .filter(hit -> hit.area().contains(x, y))
                    .map(ActionHit::launcher)
                    .findFirst();
        }

        Optional<FooterAction> footerActionAt(int x, int y) {
            return footerHits.stream()
                    .filter(hit -> hit.area().contains(x, y))
                    .map(FooterHit::action)
                    .findFirst();
        }

        OptionalInt overlayIndexAt(int x, int y) {
            return overlayHits.stream()
                    .filter(hit -> hit.area().contains(x, y))
                    .mapToInt(OverlayHit::index)
                    .findFirst();
        }
    }

    record TabHit(WorkbenchState.WorkbenchTab tab, Rect area) {
        TabHit {
            Objects.requireNonNull(tab, "tab");
            Objects.requireNonNull(area, "area");
        }
    }

    record RowHit(WorkbenchState.WorkbenchTab tab, int rowIndex, Rect area) {
        RowHit {
            Objects.requireNonNull(tab, "tab");
            Objects.requireNonNull(area, "area");
            if (!tab.hasTable() || rowIndex < 0) {
                throw new IllegalArgumentException("Row hits require a table tab and row index");
            }
        }
    }

    enum Launcher {
        CONTEXT,
        AUTH,
        MORE
    }

    record ActionHit(Launcher launcher, Rect area) {
        ActionHit {
            Objects.requireNonNull(launcher, "launcher");
            Objects.requireNonNull(area, "area");
        }
    }

    enum FooterAction {
        ACTIONS,
        CONTEXT,
        AUTH,
        MORE,
        REFRESH,
        QUIT,
        SHELL,
        BACK,
        SORT,
        OPEN,
        EDIT,
        TOGGLE_FOCUS,
        SAVE,
        SAVE_AND_CLOSE,
        CANCEL_EDIT,
        DISCARD,
        CANCEL_DISCARD,
        APPLY_PIPELINE,
        LOGS,
        INSPECT,
        START_PIPELINE,
        PAUSE_PIPELINE,
        RESUME_PIPELINE,
        STOP_PIPELINE,
        APPLY_SELECTED_ARTIFACT,
        CONFIRM,
        CANCEL_CONFIRM
    }

    record FooterHit(FooterAction action, Rect area) {
        FooterHit {
            Objects.requireNonNull(action, "action");
            Objects.requireNonNull(area, "area");
        }
    }

    record OverlayHit(int index, Rect area) {
        OverlayHit {
            Objects.requireNonNull(area, "area");
            if (index < 0) {
                throw new IllegalArgumentException("Overlay index must not be negative");
            }
        }
    }

    private record HeaderLayout(List<TabHit> tabHits, List<ActionHit> actionHits) {
    }

    private record ContentLayout(List<RowHit> rowHits, int visibleRows) {
        private ContentLayout {
            rowHits = List.copyOf(rowHits);
        }
    }

    private record FooterLayout(List<FooterHit> hits) {
        private FooterLayout {
            hits = List.copyOf(hits);
        }
    }

    private record FooterHint(String key, String label, Optional<FooterAction> action) {
    }

    private record Columns(
            int identifier,
            int alignment,
            int local,
            String separator) {

        static Columns forWidth(int width, boolean wide) {
            if (wide) {
                int local = Math.max(24, width - 40 - 18 - 6);
                return new Columns(40, 18, local, " | ");
            }
            int local = Math.max(16, Math.min(34, width / 3));
            int alignment = 14;
            int identifier = Math.max(12, width - alignment - local - 2);
            return new Columns(identifier, alignment, local, " ");
        }

        String format(
                String idValue,
                String alignmentValue,
                String localValue) {
            return pad(idValue, identifier)
                    + separator + pad(alignmentValue, alignment)
                    + separator + pad(localValue, local);
        }
    }
}
