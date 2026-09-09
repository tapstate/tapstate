package io.tapstate.cli;

import dev.tamboui.layout.Rect;
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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
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

    private WorkbenchRenderer() {
    }

    static RenderLayout render(Frame frame, WorkbenchState state) {
        return render(frame, state, DEFAULT_THEME);
    }

    static RenderLayout render(Frame frame, WorkbenchState state, WorkbenchTheme theme) {
        Objects.requireNonNull(frame, "frame");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(theme, "theme");
        Rect area = frame.area();
        frame.buffer().setStyle(area, theme.base());
        if (area.width() < MIN_WIDTH || area.height() < MIN_HEIGHT) {
            renderTooSmall(frame, area, theme);
            return RenderLayout.forTooSmallFrame();
        }

        boolean wide = area.width() >= WIDE_WIDTH;
        int footerY = area.bottom() - 1;
        HeaderLayout header = renderHeaderAndTabs(frame, area, state, wide, theme);
        Rect contentArea = new Rect(
                area.x(), area.y() + CONTENT_Y, area.width(), footerY - (area.y() + CONTENT_Y));
        ContentLayout content = renderContent(frame, contentArea, state, wide, theme);
        renderFooter(frame, area, footerY, state, theme);
        List<OverlayHit> overlayHits = state.overlay()
                .map(overlay -> renderOverlay(frame, area, overlay, theme))
                .orElseGet(List::of);
        return new RenderLayout(
                false, wide, content.visibleRows(), header.tabHits(), content.rowHits(),
                header.actionHits(), overlayHits);
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
        x += write(frame, x, area.y() + HEADER_Y, " TapState", theme.title(), area);
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
        WorkbenchState.WorkbenchTab[] tabs = WorkbenchState.WorkbenchTab.values();
        for (int index = 0; index < tabs.length; index++) {
            WorkbenchState.WorkbenchTab tab = tabs[index];
            String label = tabLabel(tab);
            String badge = tabBadge(state, tab);
            if (!badge.isEmpty()) {
                int badgeX = x + Math.max(0, (displayWidth(label) - displayWidth(badge)) / 2);
                write(frame, badgeX, area.y() + TAB_BADGES_Y, badge, theme.info(), area);
            }
            Style style = tab == state.selectedTab()
                    ? theme.accentBackground()
                    : theme.muted();
            int width = write(frame, x, area.y() + TAB_LABELS_Y, label, style, area);
            if (width > 0) {
                hits.add(new TabHit(tab, new Rect(x, area.y() + TAB_LABELS_Y, width, 1)));
            }
            x += width;
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

    private static List<OverlayHit> renderOverlay(
            Frame frame, Rect area, WorkbenchOverlayState overlay, WorkbenchTheme theme) {
        int width = Math.min(60, area.width() - 8);
        int contentRows = switch (overlay) {
            case WorkbenchOverlayState.More ignored -> 3;
            case WorkbenchOverlayState.ContextPicker picker ->
                    Math.max(4, Math.min(10, picker.contexts().size()) + 3);
            case WorkbenchOverlayState.ContextCreate ignored -> 8;
            case WorkbenchOverlayState.Login login -> transientLogin(login) ? 7 : 6;
            case WorkbenchOverlayState.Help ignored -> 6;
        };
        int height = contentRows + 2;
        int x = area.x() + (area.width() - width) / 2;
        int y = area.y() + (area.height() - height) / 2;
        Rect box = new Rect(x, y, width, height);
        frame.renderWidget(Clear.INSTANCE, box);
        Block block = Block.builder()
                .borderType(BorderType.ROUNDED)
                .borders(Borders.ALL)
                .borderStyle(theme.accent())
                .title(Title.from(Line.from(Span.styled(
                        " " + overlayTitle(overlay) + " ", theme.title()))))
                .build();
        frame.renderWidget(block, box);

        return switch (overlay) {
            case WorkbenchOverlayState.More more -> renderMore(frame, area, box, more, theme);
            case WorkbenchOverlayState.ContextPicker picker ->
                    renderContexts(frame, area, box, picker, theme);
            case WorkbenchOverlayState.ContextCreate create ->
                    renderContextCreate(frame, area, box, create, theme);
            case WorkbenchOverlayState.Login login -> renderLogin(frame, area, box, login, theme);
            case WorkbenchOverlayState.Help ignored -> renderHelp(frame, area, box, theme);
        };
    }

    private static String overlayTitle(WorkbenchOverlayState overlay) {
        return switch (overlay) {
            case WorkbenchOverlayState.More ignored -> "More";
            case WorkbenchOverlayState.ContextPicker ignored -> "Choose Context";
            case WorkbenchOverlayState.ContextCreate ignored -> "New Context";
            case WorkbenchOverlayState.Login login -> "Sign in to " + login.contextName();
            case WorkbenchOverlayState.Help ignored -> "Help";
        };
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
                frame, box.x() + 2, box.y() + box.height() - 2, message,
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
                "r refresh   q quit   Esc close", theme.base(), area);
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
        Block block = Block.builder()
                .borderType(BorderType.ROUNDED)
                .borders(Borders.ALL)
                .borderStyle(theme.accent())
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
            renderOverview(frame, inner, snapshot.overview(), theme);
            rowHits = List.of();
        } else {
            List<WorkbenchArtifactRow> rows = rows(snapshot, state.selectedTab());
            WorkbenchTableState table = table(state, state.selectedTab());
            rowHits = renderTable(
                    frame, inner, state.selectedTab(), rows, table, wide, visibleRows, theme);
        }
        write(frame, inner.x(), inner.bottom() - 1,
                notification(state), notificationStyle(state, theme), inner);
        return new ContentLayout(rowHits, visibleRows);
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
                document -> renderDocument(frame, viewerInner, document, theme),
                () -> {
                    write(frame, viewerInner.x(), viewerInner.y(),
                            "Select a file and press Enter to open it.", theme.muted(), viewerInner);
                    writeNotification(frame, viewerInner.x(), viewerInner.bottom() - 1,
                            state, theme, viewerInner);
                });
        state.workspaceView().document()
                .filter(WorkbenchWorkspaceState.Document::pendingDiscard)
                .ifPresent(ignored -> renderDiscardPopup(frame, viewerArea, theme));
        return new ContentLayout(List.copyOf(hits), capacity);
    }

    private static Block panel(String title, boolean focused, WorkbenchTheme theme) {
        return Block.builder()
                .borderType(BorderType.ROUNDED)
                .borders(Borders.ALL)
                .borderStyle(focused ? theme.accent() : theme.muted())
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
            Style activeBackground = theme.selection().bg()
                    .map(color -> Style.EMPTY.bg(color))
                    .orElse(Style.EMPTY);
            if (active) {
                frame.buffer().setStyle(new Rect(area.x(), y, area.width(), 1), activeBackground);
            }
            x += write(frame, x, y, active ? ">> " : "   ",
                    active ? theme.label().bold().patch(activeBackground) : theme.base(), area);
            x += write(frame, x, y, pad(Integer.toString(index + 1), numberWidth) + " ",
                    (active ? theme.label().bold() : theme.muted()).patch(activeBackground), area);
            renderYamlLine(frame, x, y, lines[index], theme, activeBackground, area);
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
            applyRange(styles, key.start(2), key.end(2), theme.label());
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

    private static void renderDiscardPopup(Frame frame, Rect area, WorkbenchTheme theme) {
        int width = Math.min(44, Math.max(40, area.width() - 4));
        width = Math.min(width, area.width() - 2);
        int height = 6;
        Rect popup = new Rect(
                area.x() + Math.max(0, (area.width() - width) / 2),
                area.y() + Math.max(0, (area.height() - height) / 2),
                width,
                height);
        frame.renderWidget(Clear.INSTANCE, popup);
        Block block = Block.builder()
                .borderType(BorderType.ROUNDED)
                .borders(Borders.ALL)
                .borderStyle(theme.warning())
                .title(Title.from(Line.from(Span.styled(
                        " Discard Changes? ", theme.warning().bold()))))
                .build();
        frame.renderWidget(block, popup);
        Rect inner = block.inner(popup);
        writeCentered(frame, inner, inner.y() + 1, "Unsaved changes will be lost.", theme.base());
        writeCentered(frame, inner, inner.y() + 3, "Enter confirm    Esc cancel", theme.base());
    }

    private static void renderOverview(
            Frame frame, Rect area, WorkbenchOverviewSnapshot overview, WorkbenchTheme theme) {
        write(frame, area.x(), area.y(), "Resources", theme.label().bold(), area);
        int rowsY = area.y() + 1;
        int summaryY = area.bottom() - 4;
        int kindCapacity = Math.max(1, summaryY - rowsY);
        boolean overflow = overview.kinds().size() > kindCapacity;
        int visibleKinds = Math.min(
                overview.kinds().size(),
                overflow ? kindCapacity - 1 : kindCapacity);
        int y = rowsY;
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

    private static List<RowHit> renderTable(
            Frame frame,
            Rect area,
            WorkbenchState.WorkbenchTab tab,
            List<WorkbenchArtifactRow> rows,
            WorkbenchTableState table,
            boolean wide,
            int visibleRows,
            WorkbenchTheme theme) {
        Columns columns = Columns.forWidth(area.width(), wide);
        renderTableHeader(frame, area, columns, table, theme);
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
            int width = write(frame, area.x(), y, line, style, area);
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

    private static void renderFooter(
            Frame frame, Rect area, int y, WorkbenchState state, WorkbenchTheme theme) {
        boolean hasSelectableRows = state.selectedTab().hasTable()
                && state.snapshot()
                        .map(snapshot -> !rows(snapshot, state.selectedTab()).isEmpty())
                        .orElse(false);
        List<FooterHint> hints = switch (state.selectedTab()) {
            case OVERVIEW -> List.of(
                    new FooterHint("1-4", "views"),
                    new FooterHint("c", "context"),
                    new FooterHint("a", "auth"),
                    new FooterHint("0", "more"),
                    new FooterHint("r", "refresh"),
                    new FooterHint("q", "quit"));
            case WORKSPACE -> hasSelectableRows
                    ? workspaceFooter(state)
                    : List.of(
                            new FooterHint("Esc", "back"),
                            new FooterHint("r", "refresh"),
                            new FooterHint("q", "quit"));
            case SOURCES, PIPELINES -> hasSelectableRows
                    ? List.of(
                            new FooterHint("↑↓", "navigate"),
                            new FooterHint("Esc", "back"),
                            new FooterHint("s", "sort"),
                            new FooterHint("r", "refresh"),
                            new FooterHint("q", "quit"))
                    : List.of(
                            new FooterHint("Esc", "back"),
                            new FooterHint("r", "refresh"),
                            new FooterHint("q", "quit"));
        };
        int x = area.x();
        for (FooterHint hint : hints) {
            x += write(frame, x, y, " " + hint.key() + " ", theme.hintKey(), area);
            x += write(frame, x, y, hint.label() + "  ", theme.base(), area);
            if (x >= area.right()) {
                return;
            }
        }
    }

    private static List<FooterHint> workspaceFooter(WorkbenchState state) {
        if (state.workspaceView().document()
                .map(WorkbenchWorkspaceState.Document::pendingDiscard)
                .orElse(false)) {
            return List.of(
                    new FooterHint("Enter", "confirm"),
                    new FooterHint("Esc", "cancel"));
        }
        if (state.workspaceView().editing()) {
            return List.of(
                    new FooterHint("↑↓←→", "navigate"),
                    new FooterHint("Esc", "cancel"),
                    new FooterHint("Ctrl+S", "save"),
                    new FooterHint("F5", "save & close"));
        }
        if (state.workspaceView().focus() == WorkbenchWorkspaceState.Focus.VIEWER) {
            return List.of(
                    new FooterHint("↑↓", "navigate"),
                    new FooterHint("Esc", "back"),
                    new FooterHint("F4", "edit"),
                    new FooterHint("Tab", "files"));
        }
        List<FooterHint> hints = new ArrayList<>(List.of(
                new FooterHint("↑↓", "navigate"),
                new FooterHint("Esc", "back"),
                new FooterHint("Enter", "open"),
                new FooterHint("F4", "edit")));
        if (state.workspaceView().document().isPresent()) {
            hints.add(new FooterHint("Tab", "viewer"));
        }
        return List.copyOf(hints);
    }

    private static String tabLabel(WorkbenchState.WorkbenchTab tab) {
        return switch (tab) {
            case OVERVIEW -> "🌊  1 Overview";
            case WORKSPACE -> "📁  2 Workspace";
            case SOURCES -> "🔌  3 Sources";
            case PIPELINES -> "🔀  4 Pipelines";
        };
    }

    private static String tabBadge(WorkbenchState state, WorkbenchState.WorkbenchTab tab) {
        if (tab == WorkbenchState.WorkbenchTab.OVERVIEW) {
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
        };
    }

    private static List<WorkbenchArtifactRow> rows(
            WorkbenchSnapshot snapshot, WorkbenchState.WorkbenchTab tab) {
        return switch (tab) {
            case OVERVIEW -> List.of();
            case WORKSPACE -> snapshot.workspace().rows();
            case SOURCES -> snapshot.sources().rows();
            case PIPELINES -> snapshot.pipelines().rows();
        };
    }

    private static WorkbenchTableState table(
            WorkbenchState state, WorkbenchState.WorkbenchTab tab) {
        return switch (tab) {
            case OVERVIEW -> WorkbenchTableState.empty();
            case WORKSPACE -> state.workspaceTable();
            case SOURCES -> state.sourcesTable();
            case PIPELINES -> state.pipelinesTable();
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
            List<OverlayHit> overlayHits) {

        RenderLayout {
            if (visibleRowCapacity < 0) {
                throw new IllegalArgumentException("Visible row capacity must not be negative");
            }
            tabHits = List.copyOf(tabHits);
            rowHits = List.copyOf(rowHits);
            actionHits = List.copyOf(actionHits);
            overlayHits = List.copyOf(overlayHits);
        }

        static RenderLayout forTooSmallFrame() {
            return new RenderLayout(true, false, 0, List.of(), List.of(), List.of(), List.of());
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

    private record FooterHint(String key, String label) {
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
