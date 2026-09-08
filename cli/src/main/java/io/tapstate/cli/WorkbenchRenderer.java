package io.tapstate.cli;

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

/** Pure, snapshot-only workbench renderer with an immutable pointer hit map. */
final class WorkbenchRenderer {

    private static final int MIN_WIDTH = 88;
    private static final int MIN_HEIGHT = 24;
    private static final int WIDE_WIDTH = 157;
    private static final int TABS_Y = 5;
    private static final int CONTENT_TITLE_Y = 7;
    private static final int TABLE_HEADER_Y = 8;
    private static final int TABLE_ROWS_Y = 9;

    private WorkbenchRenderer() {
    }

    static RenderLayout render(Frame frame, WorkbenchState state) {
        Objects.requireNonNull(frame, "frame");
        Objects.requireNonNull(state, "state");
        Rect area = frame.area();
        if (area.width() < MIN_WIDTH || area.height() < MIN_HEIGHT) {
            renderTooSmall(frame, area);
            return RenderLayout.forTooSmallFrame();
        }

        boolean wide = area.width() >= WIDE_WIDTH;
        int notificationY = area.bottom() - 2;
        int footerY = area.bottom() - 1;
        int visibleRows = Math.max(1, notificationY - (area.y() + TABLE_ROWS_Y));
        List<TabHit> tabHits = renderHeaderAndTabs(frame, area, state, wide);
        List<RowHit> rowHits = renderContent(frame, area, state, wide, visibleRows);
        write(frame, area.x(), notificationY, notification(state), Style.EMPTY.dim(), area);
        write(frame, area.x(), footerY, footer(state), Style.EMPTY.dim(), area);
        return new RenderLayout(false, wide, visibleRows, tabHits, rowHits);
    }

    private static void renderTooSmall(Frame frame, Rect area) {
        String title = "Terminal size too small:";
        String actual = "Width = " + area.width() + "  Height = " + area.height();
        String needed = "Needed for current config:";
        String minimum = "Width = " + MIN_WIDTH + "  Height = " + MIN_HEIGHT;
        int startY = area.y() + Math.max(0, (area.height() - 5) / 2);
        writeCentered(frame, area, startY, title, Style.EMPTY.bold());
        writeCentered(frame, area, startY + 1, actual, Style.EMPTY);
        writeCentered(frame, area, startY + 3, needed, Style.EMPTY.bold());
        writeCentered(frame, area, startY + 4, minimum, Style.EMPTY);
    }

    private static List<TabHit> renderHeaderAndTabs(
            Frame frame, Rect area, WorkbenchState state, boolean wide) {
        WorkbenchSessionSnapshot session = state.snapshot()
                .map(WorkbenchSnapshot::session)
                .orElseGet(WorkbenchSessionSnapshot::empty);
        String title = session.versions().isBlank()
                ? "Tapstate workbench"
                : "Tapstate workbench  " + session.versions();
        write(frame, area.x(), area.y(), title, Style.EMPTY.bold(), area);
        write(frame, area.x(), area.y() + 1,
                "Workspace: " + displayPath(session.workspaceRoot()), Style.EMPTY, area);
        write(frame, area.x(), area.y() + 2,
                "Context: " + displayContext(session)
                        + separator(wide) + "Connection: " + words(session.connection())
                        + separator(wide) + "Auth: " + words(session.authentication()),
                Style.EMPTY, area);
        write(frame, area.x(), area.y() + 3,
                "Principal: " + session.principal().orElse("-")
                        + separator(wide) + "Endpoint: " + session.landingNode().map(URI::toString).orElse("-"),
                Style.EMPTY, area);

        int x = area.x();
        List<TabHit> hits = new ArrayList<>();
        WorkbenchState.WorkbenchTab[] tabs = WorkbenchState.WorkbenchTab.values();
        for (int index = 0; index < tabs.length; index++) {
            WorkbenchState.WorkbenchTab tab = tabs[index];
            String label = tab.displayLabel();
            Style style = tab == state.selectedTab()
                    ? Style.EMPTY.bold().reversed()
                    : Style.EMPTY.dim();
            int width = write(frame, x, area.y() + TABS_Y, label, style, area);
            if (width > 0) {
                hits.add(new TabHit(tab, new Rect(x, area.y() + TABS_Y, width, 1)));
            }
            x += width;
            if (index + 1 < tabs.length) {
                x += write(frame, x, area.y() + TABS_Y, " | ", Style.EMPTY.dim(), area);
            }
        }
        return List.copyOf(hits);
    }

    private static List<RowHit> renderContent(
            Frame frame,
            Rect area,
            WorkbenchState state,
            boolean wide,
            int visibleRows) {
        write(frame, area.x(), area.y() + CONTENT_TITLE_Y,
                state.selectedTab().label(), Style.EMPTY.bold(), area);
        if (state.snapshot().isEmpty()) {
            write(frame, area.x(), area.y() + TABLE_ROWS_Y,
                    state.selectedTab().emptyMessage(), Style.EMPTY, area);
            return List.of();
        }

        WorkbenchSnapshot snapshot = state.snapshot().orElseThrow();
        if (state.selectedTab() == WorkbenchState.WorkbenchTab.OVERVIEW) {
            renderOverview(frame, area, snapshot.overview());
            return List.of();
        }
        List<WorkbenchArtifactRow> rows = rows(snapshot, state.selectedTab());
        WorkbenchTableState table = table(state, state.selectedTab());
        return renderTable(frame, area, state.selectedTab(), rows, table, wide, visibleRows);
    }

    private static void renderOverview(Frame frame, Rect area, WorkbenchOverviewSnapshot overview) {
        write(frame, area.x(), area.y() + TABLE_HEADER_Y,
                "Kinds", Style.EMPTY.bold(), area);
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
                    Style.EMPTY, area);
        }
        if (overflow) {
            write(frame, area.x(), y,
                    "+" + (overview.kinds().size() - visibleKinds) + " kinds not shown",
                    Style.EMPTY.dim(), area);
        }
        WorkbenchAlignmentCounts counts = overview.alignment();
        write(frame, area.x(), summaryY,
                "Alignment: local only " + counts.localOnly()
                        + " | remote only " + counts.remoteOnly()
                        + " | in sync " + counts.inSync(),
                Style.EMPTY, area);
        write(frame, area.x(), summaryY + 1,
                "           drifted " + counts.drifted()
                        + " | invalid local " + counts.invalidLocal()
                        + " | unknown " + counts.unknown(),
                Style.EMPTY, area);
    }

    private static List<RowHit> renderTable(
            Frame frame,
            Rect area,
            WorkbenchState.WorkbenchTab tab,
            List<WorkbenchArtifactRow> rows,
            WorkbenchTableState table,
            boolean wide,
            int visibleRows) {
        Columns columns = Columns.forWidth(area.width(), wide);
        write(frame, area.x(), area.y() + TABLE_HEADER_Y,
                columns.format("KIND", "IDENTIFIER", "ALIGNMENT", "LOCAL", "REMOTE"),
                Style.EMPTY.bold(), area);
        if (rows.isEmpty()) {
            write(frame, area.x(), area.y() + TABLE_ROWS_Y,
                    emptyRowsMessage(tab), Style.EMPTY, area);
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
            Style style = index == selected ? Style.EMPTY.reversed() : Style.EMPTY;
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
        return "1-4 tabs  Left/Right switch"
                + (hasSelectableRows ? "  Up/Down select" : "")
                + "  r refresh  q quit";
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
            List<RowHit> rowHits) {

        RenderLayout {
            if (visibleRowCapacity < 0) {
                throw new IllegalArgumentException("Visible row capacity must not be negative");
            }
            tabHits = List.copyOf(tabHits);
            rowHits = List.copyOf(rowHits);
        }

        static RenderLayout forTooSmallFrame() {
            return new RenderLayout(true, false, 0, List.of(), List.of());
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
