package io.tapstate.cli;

import dev.tamboui.buffer.Cell;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Style;
import dev.tamboui.terminal.Frame;
import dev.tamboui.text.CharWidth;

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/** Pure, snapshot-only workbench renderer with an immutable pointer hit map. */
final class WorkbenchRenderer {

    private static final int MIN_WIDTH = 88;
    private static final int MIN_HEIGHT = 24;
    private static final int WIDE_WIDTH = 157;
    private static final int TABS_Y = 5;
    private static final int CONTENT_TITLE_Y = 7;
    private static final int TABLE_HEADER_Y = 8;
    private static final int TABLE_ROWS_Y = 9;
    private static final WorkbenchTheme DEFAULT_THEME = WorkbenchTheme.dark();

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
        int notificationY = area.bottom() - 2;
        int footerY = area.bottom() - 1;
        int visibleRows = Math.max(1, notificationY - (area.y() + TABLE_ROWS_Y));
        HeaderLayout header = renderHeaderAndTabs(frame, area, state, wide, theme);
        List<RowHit> rowHits = renderContent(frame, area, state, wide, visibleRows, theme);
        write(frame, area.x(), notificationY, notification(state), notificationStyle(state, theme), area);
        write(frame, area.x(), footerY, footer(state), theme.muted(), area);
        List<OverlayHit> overlayHits = state.overlay()
                .map(overlay -> renderOverlay(frame, area, overlay, theme))
                .orElseGet(List::of);
        return new RenderLayout(
                false, wide, visibleRows, header.tabHits(), rowHits, header.actionHits(), overlayHits);
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
        String title = session.versions().isBlank()
                ? "Tapstate workbench"
                : "Tapstate workbench  " + session.versions();
        write(frame, area.x(), area.y(), title, theme.title(), area);
        write(frame, area.x(), area.y() + 1,
                "Workspace: " + displayPath(session.workspaceRoot()), theme.base(), area);
        String contextLine = "Context: " + displayContext(session)
                        + separator(wide) + "Connection: " + words(session.connection())
                        + separator(wide) + "Auth: " + words(session.authentication());
        int contextWidth = write(frame, area.x(), area.y() + 2, contextLine, theme.info(), area);
        write(frame, area.x(), area.y() + 3,
                "Principal: " + session.principal().orElse("-")
                        + separator(wide) + "Endpoint: " + session.landingNode().map(URI::toString).orElse("-"),
                theme.base(), area);

        int x = area.x();
        List<TabHit> hits = new ArrayList<>();
        List<ActionHit> actions = new ArrayList<>();
        if (contextWidth > 0) {
            actions.add(new ActionHit(
                    Launcher.CONTEXT,
                    new Rect(area.x(), area.y() + 2, contextWidth, 1)));
        }
        WorkbenchState.WorkbenchTab[] tabs = WorkbenchState.WorkbenchTab.values();
        for (int index = 0; index < tabs.length; index++) {
            WorkbenchState.WorkbenchTab tab = tabs[index];
            String label = tabLabel(state, tab);
            Style style = tab == state.selectedTab()
                    ? theme.accentBackground()
                    : theme.muted();
            int width = write(frame, x, area.y() + TABS_Y, label, style, area);
            if (width > 0) {
                hits.add(new TabHit(tab, new Rect(x, area.y() + TABS_Y, width, 1)));
            }
            x += width;
            if (index + 1 < tabs.length) {
                x += write(frame, x, area.y() + TABS_Y, " | ", theme.muted(), area);
            }
        }
        int separatorWidth = write(frame, x, area.y() + TABS_Y, " | ", theme.muted(), area);
        int moreX = x + separatorWidth;
        int moreWidth = write(frame, moreX, area.y() + TABS_Y, "0 More", theme.accent(), area);
        if (moreWidth > 0) {
            actions.add(new ActionHit(
                    Launcher.MORE,
                    new Rect(moreX, area.y() + TABS_Y, moreWidth, 1)));
        }
        return new HeaderLayout(List.copyOf(hits), List.copyOf(actions));
    }

    private static List<OverlayHit> renderOverlay(
            Frame frame, Rect area, WorkbenchOverlayState overlay, WorkbenchTheme theme) {
        int width = Math.min(60, area.width() - 8);
        int contentRows = switch (overlay) {
            case WorkbenchOverlayState.More ignored -> 3;
            case WorkbenchOverlayState.ContextPicker picker ->
                    Math.max(3, Math.min(10, picker.contexts().size()) + 2);
            case WorkbenchOverlayState.Login ignored -> 6;
            case WorkbenchOverlayState.Help ignored -> 6;
        };
        int height = contentRows + 2;
        int x = area.x() + (area.width() - width) / 2;
        int y = area.y() + (area.height() - height) / 2;
        Rect box = new Rect(x, y, width, height);
        frame.buffer().fill(box, new Cell(" ", theme.base()));
        write(frame, x, y, "+" + "-".repeat(width - 2) + "+", theme.accent(), area);
        for (int row = 1; row < height - 1; row++) {
            write(frame, x, y + row, "|", theme.accent(), area);
            write(frame, x + width - 1, y + row, "|", theme.accent(), area);
        }
        write(frame, x, y + height - 1,
                "+" + "-".repeat(width - 2) + "+", theme.accent(), area);

        return switch (overlay) {
            case WorkbenchOverlayState.More more -> renderMore(frame, area, box, more, theme);
            case WorkbenchOverlayState.ContextPicker picker ->
                    renderContexts(frame, area, box, picker, theme);
            case WorkbenchOverlayState.Login login -> renderLogin(frame, area, box, login, theme);
            case WorkbenchOverlayState.Help ignored -> renderHelp(frame, area, box, theme);
        };
    }

    private static List<OverlayHit> renderMore(
            Frame frame, Rect area, Rect box, WorkbenchOverlayState.More more, WorkbenchTheme theme) {
        write(frame, box.x() + 2, box.y() + 1, "More", theme.title(), area);
        List<String> entries = List.of("Context & Auth", "Help");
        List<OverlayHit> hits = new ArrayList<>();
        for (int index = 0; index < entries.size(); index++) {
            String line = (index == more.selectedIndex() ? "> " : "  ") + entries.get(index);
            int rowY = box.y() + 2 + index;
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
        write(frame, box.x() + 2, box.y() + 1, "Choose context", theme.title(), area);
        if (picker.contexts().isEmpty()) {
            write(frame, box.x() + 2, box.y() + 2,
                    "No saved contexts. Use tapstate context create first.", theme.warning(), area);
            return List.of();
        }
        List<OverlayHit> hits = new ArrayList<>();
        int visible = Math.min(10, picker.contexts().size());
        for (int index = 0; index < visible; index++) {
            WorkbenchActionGateway.ContextOption context = picker.contexts().get(index);
            String line = (index == picker.selectedIndex() ? "> " : "  ")
                    + context.name() + (context.suggested() ? "  suggested" : "");
            int rowY = box.y() + 2 + index;
            int width = write(frame, box.x() + 2, rowY, line,
                    index == picker.selectedIndex() ? theme.selection() : theme.base(), area);
            hits.add(new OverlayHit(index, new Rect(box.x() + 2, rowY, width, 1)));
        }
        picker.message().ifPresent(message -> write(
                frame, box.x() + 2, box.y() + box.height() - 2, message,
                picker.pending() ? theme.info() : theme.warning(), area));
        return List.copyOf(hits);
    }

    private static List<OverlayHit> renderLogin(
            Frame frame, Rect area, Rect box, WorkbenchOverlayState.Login login, WorkbenchTheme theme) {
        write(frame, box.x() + 2, box.y() + 1,
                "Sign in to " + login.contextName(), theme.title(), area);
        write(frame, box.x() + 2, box.y() + 2,
                (login.stage() == WorkbenchOverlayState.Login.Stage.USERNAME ? "> " : "  ")
                        + "Username: " + login.username(),
                login.stage() == WorkbenchOverlayState.Login.Stage.USERNAME
                        ? theme.selection() : theme.base(), area);
        write(frame, box.x() + 2, box.y() + 3,
                (login.stage() == WorkbenchOverlayState.Login.Stage.PASSWORD ? "> " : "  ")
                        + "Password: " + login.password().mask(),
                login.stage() == WorkbenchOverlayState.Login.Stage.PASSWORD
                        ? theme.selection() : theme.base(), area);
        String hint = login.pending()
                ? "Signing in..."
                : login.stage() == WorkbenchOverlayState.Login.Stage.USERNAME
                        ? "Enter next  Esc cancel"
                        : "Enter sign in  Esc cancel";
        write(frame, box.x() + 2, box.y() + 4, hint, theme.muted(), area);
        login.message().ifPresent(message -> write(
                frame, box.x() + 2, box.y() + 5, message, theme.error(), area));
        return List.of();
    }

    private static List<OverlayHit> renderHelp(
            Frame frame, Rect area, Rect box, WorkbenchTheme theme) {
        write(frame, box.x() + 2, box.y() + 1, "Help", theme.title(), area);
        write(frame, box.x() + 2, box.y() + 2, "1-4 switch views", theme.base(), area);
        write(frame, box.x() + 2, box.y() + 3, "c choose context or sign in", theme.base(), area);
        write(frame, box.x() + 2, box.y() + 4,
                "r refresh   q quit   Esc close", theme.base(), area);
        return List.of();
    }

    private static List<RowHit> renderContent(
            Frame frame,
            Rect area,
            WorkbenchState state,
            boolean wide,
            int visibleRows,
            WorkbenchTheme theme) {
        write(frame, area.x(), area.y() + CONTENT_TITLE_Y,
                state.selectedTab().label(), theme.title(), area);
        if (state.snapshot().isEmpty()) {
            write(frame, area.x(), area.y() + TABLE_ROWS_Y,
                    state.selectedTab().emptyMessage(), theme.base(), area);
            return List.of();
        }

        WorkbenchSnapshot snapshot = state.snapshot().orElseThrow();
        if (state.selectedTab() == WorkbenchState.WorkbenchTab.OVERVIEW) {
            renderOverview(frame, area, snapshot.overview(), theme);
            return List.of();
        }
        List<WorkbenchArtifactRow> rows = rows(snapshot, state.selectedTab());
        WorkbenchTableState table = table(state, state.selectedTab());
        return renderTable(frame, area, state.selectedTab(), rows, table, wide, visibleRows, theme);
    }

    private static void renderOverview(
            Frame frame, Rect area, WorkbenchOverviewSnapshot overview, WorkbenchTheme theme) {
        write(frame, area.x(), area.y() + TABLE_HEADER_Y,
                "Resources", theme.label().bold(), area);
        int rowsY = area.y() + TABLE_ROWS_Y;
        int summaryY = area.bottom() - 5;
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
        write(frame, area.x(), area.y() + TABLE_HEADER_Y,
                columns.format("KIND", "IDENTIFIER", "ALIGNMENT", "LOCAL", "REMOTE"),
                theme.label().bold(), area);
        if (rows.isEmpty()) {
            write(frame, area.x(), area.y() + TABLE_ROWS_Y,
                    emptyRowsMessage(tab), theme.base(), area);
            return List.of();
        }

        int selected = Math.clamp(table.selectedIndex(), 0, rows.size() - 1);
        int maximumScroll = Math.max(0, rows.size() - visibleRows);
        int scroll = Math.clamp(table.scrollOffset(), 0, maximumScroll);
        if (selected < scroll) {
            scroll = selected;
        } else if (selected >= scroll + visibleRows) {
            scroll = selected - visibleRows + 1;
        }

        List<RowHit> hits = new ArrayList<>();
        int limit = Math.min(rows.size(), scroll + visibleRows);
        for (int index = scroll; index < limit; index++) {
            WorkbenchArtifactRow row = rows.get(index);
            String line = columns.format(
                    row.key().kind(),
                    row.key().id(),
                    words(row.alignment()),
                    localMarker(row),
                    remoteMarker(row));
            int y = area.y() + TABLE_ROWS_Y + index - scroll;
            Style style = index == selected ? theme.selection() : theme.base();
            int width = write(frame, area.x(), y, line, style, area);
            if (width > 0) {
                hits.add(new RowHit(tab, index, new Rect(area.x(), y, width, 1)));
            }
        }
        return List.copyOf(hits);
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
                    : "Remote artifacts: " + available.artifactCount();
            case WorkbenchRemoteState.NotConfigured ignored -> "No context selected";
            case WorkbenchRemoteState.SignedOut ignored -> "Signed out";
            case WorkbenchRemoteState.Offline ignored -> "Server offline";
            case WorkbenchRemoteState.Rejected rejected ->
                    "Remote rejected: " + rejected.code() + ", " + rejected.message();
            case WorkbenchRemoteState.Diagnostic diagnostic ->
                    "Diagnostic: " + diagnostic.code().code();
        };
    }

    private static String footer(WorkbenchState state) {
        boolean hasSelectableRows = state.selectedTab().hasTable()
                && state.snapshot()
                        .map(snapshot -> !rows(snapshot, state.selectedTab()).isEmpty())
                        .orElse(false);
        return "1-4 views  c context  0 more"
                + (hasSelectableRows ? "  Up/Down select" : "")
                + "  r refresh  q quit";
    }

    private static String tabLabel(WorkbenchState state, WorkbenchState.WorkbenchTab tab) {
        if (tab == WorkbenchState.WorkbenchTab.OVERVIEW) {
            return tab.displayLabel();
        }
        String badge = state.snapshot().map(snapshot -> switch (tab) {
            case OVERVIEW -> "";
            case WORKSPACE -> Integer.toString(snapshot.workspace().rows().size());
            case SOURCES -> snapshot.sources().remoteState() instanceof WorkbenchRemoteState.Available
                    ? Integer.toString(snapshot.sources().rows().size())
                    : "?";
            case PIPELINES -> snapshot.pipelines().remoteState() instanceof WorkbenchRemoteState.Available
                    ? Integer.toString(snapshot.pipelines().rows().size())
                    : "?";
        }).orElse("?");
        return tab.displayLabel() + " [" + badge + ']';
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

    private static String remoteMarker(WorkbenchArtifactRow row) {
        return switch (row.remote().size()) {
            case 0 -> "-";
            case 1 -> "remote";
            default -> row.remote().size() + " remote";
        };
    }

    private static String emptyRowsMessage(WorkbenchState.WorkbenchTab tab) {
        return switch (tab) {
            case OVERVIEW -> "No overview data.";
            case WORKSPACE -> "No workspace artifacts.";
            case SOURCES -> "No sources.";
            case PIPELINES -> "No pipelines.";
        };
    }

    private static String displayContext(WorkbenchSessionSnapshot session) {
        if (session.contextName().isEmpty()) {
            return "-";
        }
        String source = session.contextSource().map(WorkbenchRenderer::words).orElse("unknown");
        return session.contextName().orElseThrow() + " (" + source + ')';
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

    private static String separator(boolean wide) {
        return wide ? " | " : "  ";
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

    private record Columns(
            int kind,
            int identifier,
            int alignment,
            int local,
            int remote,
            String separator) {

        static Columns forWidth(int width, boolean wide) {
            if (wide) {
                int remote = Math.max(8, width - 14 - 36 - 18 - 55 - 12);
                return new Columns(14, 36, 18, 55, remote, " | ");
            }
            int remote = 8;
            int local = Math.max(12, Math.min(30, width / 3));
            int kind = 8;
            int alignment = 14;
            int identifier = Math.max(12, width - kind - alignment - local - remote - 4);
            return new Columns(kind, identifier, alignment, local, remote, " ");
        }

        String format(
                String kindValue,
                String idValue,
                String alignmentValue,
                String localValue,
                String remoteValue) {
            return pad(kindValue, kind)
                    + separator + pad(idValue, identifier)
                    + separator + pad(alignmentValue, alignment)
                    + separator + pad(localValue, local)
                    + separator + pad(remoteValue, remote);
        }
    }
}
