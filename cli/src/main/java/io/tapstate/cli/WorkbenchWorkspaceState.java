package io.tapstate.cli;

import dev.tamboui.tui.event.KeyEvent;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

/** Immutable focus and document state for the Camel-style workspace browser. */
record WorkbenchWorkspaceState(Focus focus, Optional<Document> document) {

    WorkbenchWorkspaceState {
        Objects.requireNonNull(focus, "focus");
        Objects.requireNonNull(document, "document");
        if (focus == Focus.VIEWER && document.isEmpty()) {
            throw new IllegalArgumentException("Viewer focus requires an open document");
        }
    }

    static WorkbenchWorkspaceState empty() {
        return new WorkbenchWorkspaceState(Focus.FILES, Optional.empty());
    }

    WorkbenchWorkspaceState open(Path relativePath, String content) {
        return new WorkbenchWorkspaceState(
                Focus.VIEWER, Optional.of(Document.open(relativePath, content)));
    }

    WorkbenchWorkspaceState toggleFocus() {
        if (document.isEmpty() || document.orElseThrow().editing()) {
            return this;
        }
        return new WorkbenchWorkspaceState(
                focus == Focus.FILES ? Focus.VIEWER : Focus.FILES, document);
    }

    WorkbenchWorkspaceState focusFiles() {
        return focus == Focus.FILES || editing()
                ? this : new WorkbenchWorkspaceState(Focus.FILES, document);
    }

    WorkbenchWorkspaceState edit() {
        if (document.isEmpty()) {
            return this;
        }
        return new WorkbenchWorkspaceState(Focus.VIEWER, Optional.of(document.orElseThrow().edit()));
    }

    WorkbenchWorkspaceState edit(KeyEvent key) {
        if (!editing()) {
            return this;
        }
        return new WorkbenchWorkspaceState(Focus.VIEWER, Optional.of(document.orElseThrow().edit(key)));
    }

    WorkbenchWorkspaceState navigate(KeyEvent key) {
        if (document.isEmpty() || editing()) {
            return this;
        }
        return new WorkbenchWorkspaceState(
                Focus.VIEWER, Optional.of(document.orElseThrow().navigate(key)));
    }

    WorkbenchWorkspaceState paste(String text) {
        if (!editing()) {
            return this;
        }
        return new WorkbenchWorkspaceState(Focus.VIEWER, Optional.of(document.orElseThrow().insert(text)));
    }

    WorkbenchWorkspaceState cancelEdit() {
        if (!editing()) {
            return this;
        }
        return new WorkbenchWorkspaceState(Focus.VIEWER, Optional.of(document.orElseThrow().cancelEdit()));
    }

    WorkbenchWorkspaceState requestCancelEdit() {
        if (!editing()) {
            return this;
        }
        return new WorkbenchWorkspaceState(
                Focus.VIEWER, Optional.of(document.orElseThrow().requestCancelEdit()));
    }

    WorkbenchWorkspaceState saved(boolean closeEditor) {
        if (document.isEmpty()) {
            return this;
        }
        return new WorkbenchWorkspaceState(
                Focus.VIEWER, Optional.of(document.orElseThrow().saved(closeEditor)));
    }

    WorkbenchWorkspaceState back() {
        if (editing()) {
            return cancelEdit();
        }
        if (focus == Focus.VIEWER) {
            return new WorkbenchWorkspaceState(Focus.FILES, document);
        }
        if (document.isPresent()) {
            return empty();
        }
        return this;
    }

    boolean editing() {
        return document.map(Document::editing).orElse(false);
    }

    enum Focus {
        FILES,
        VIEWER
    }

    record Document(
            Path relativePath,
            String originalContent,
            String content,
            int selectedLine,
            int cursorOffset,
            boolean editing,
            boolean pendingDiscard,
            Optional<WorkbenchQuickDocProvider.QuickDoc> quickDoc) {

        Document {
            Objects.requireNonNull(relativePath, "relativePath");
            Objects.requireNonNull(originalContent, "originalContent");
            Objects.requireNonNull(content, "content");
            Objects.requireNonNull(quickDoc, "quickDoc");
            if (relativePath.isAbsolute() || relativePath.normalize().startsWith("..")) {
                throw new IllegalArgumentException("Workspace documents require a relative path");
            }
            if (cursorOffset < 0 || cursorOffset > content.length()) {
                throw new IllegalArgumentException("Cursor is outside the document");
            }
            if (selectedLine < 0 || selectedLine >= lineCount(content)) {
                throw new IllegalArgumentException("Selected line is outside the document");
            }
            if (pendingDiscard && (!editing || !dirty(originalContent, content))) {
                throw new IllegalArgumentException("Discard confirmation requires dirty edit mode");
            }
        }

        Document(
                Path relativePath,
                String originalContent,
                String content,
                int selectedLine,
                int cursorOffset,
                boolean editing,
                boolean pendingDiscard) {
            this(relativePath, originalContent, content, selectedLine, cursorOffset, editing, pendingDiscard,
                    WorkbenchQuickDocProvider.BUNDLED.document(
                            content, editing ? lineOfOffset(content, cursorOffset) : selectedLine));
        }

        static Document open(Path relativePath, String content) {
            return new Document(relativePath.normalize(), content, content, 0, 0, false, false);
        }

        Document edit() {
            return editing ? this : new Document(
                    relativePath, content, content, selectedLine,
                    lineStart(content, selectedLine), true, false);
        }

        Document cancelEdit() {
            int line = Math.min(selectedLine, lineCount(originalContent) - 1);
            return new Document(
                    relativePath, originalContent, originalContent, line,
                    lineStart(originalContent, line), false, false);
        }

        Document requestCancelEdit() {
            if (!editing) {
                return this;
            }
            if (!dirty()) {
                return cancelEdit();
            }
            return new Document(
                    relativePath, originalContent, content, selectedLine,
                    cursorOffset, true, true);
        }

        Document saved(boolean closeEditor) {
            int line = cursorLine();
            return new Document(
                    relativePath, content, content, line, cursorOffset, !closeEditor, false);
        }

        boolean dirty() {
            return dirty(originalContent, content);
        }

        int cursorLine() {
            return lineOfOffset(content, cursorOffset);
        }

        int cursorColumn() {
            return cursorOffset - lineStart(cursorOffset);
        }

        int scopeLine() {
            if (!editing) {
                return -1;
            }
            String[] lines = content.split("\\n", -1);
            int row = cursorLine();
            String current = lines[row];
            int currentIndent = current.isBlank() ? cursorColumn() : leadingSpaces(current);
            for (int index = row - 1; index >= 0; index--) {
                String candidate = lines[index];
                String trimmed = candidate.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                int indent = leadingSpaces(candidate);
                if (indent < currentIndent && yamlScope(trimmed)) {
                    return index;
                }
            }
            return -1;
        }

        Document insert(String value) {
            StringBuilder safe = new StringBuilder();
            value.codePoints()
                    .filter(codePoint -> codePoint == '\n' || codePoint == '\t'
                            || !Character.isISOControl(codePoint))
                    .forEach(safe::appendCodePoint);
            if (safe.isEmpty()) {
                return this;
            }
            String inserted = safe.toString();
            return with(content.substring(0, cursorOffset) + inserted + content.substring(cursorOffset),
                    cursorOffset + inserted.length());
        }

        Document edit(KeyEvent key) {
            if (pendingDiscard) {
                if (key.isConfirm()) {
                    return cancelEdit();
                }
                if (key.isCancel()) {
                    return new Document(
                            relativePath, originalContent, content, selectedLine,
                            cursorOffset, true, false);
                }
                return this;
            }
            if (key.isDeleteBackward()) {
                if (cursorOffset == 0) {
                    return this;
                }
                int previous = content.offsetByCodePoints(cursorOffset, -1);
                return with(content.substring(0, previous) + content.substring(cursorOffset), previous);
            }
            if (key.isDeleteForward()) {
                if (cursorOffset == content.length()) {
                    return this;
                }
                int next = content.offsetByCodePoints(cursorOffset, 1);
                return with(content.substring(0, cursorOffset) + content.substring(next), cursorOffset);
            }
            if (key.isLeft()) {
                return cursorOffset == 0 ? this : move(content.offsetByCodePoints(cursorOffset, -1));
            }
            if (key.isRight()) {
                return cursorOffset == content.length()
                        ? this : move(content.offsetByCodePoints(cursorOffset, 1));
            }
            if (key.isHome()) {
                return move(lineStart(cursorOffset));
            }
            if (key.isEnd()) {
                return move(lineEnd(cursorOffset));
            }
            if (key.isUp() || key.isDown()) {
                int start = lineStart(cursorOffset);
                int column = cursorOffset - start;
                if (key.isUp()) {
                    if (start == 0) {
                        return this;
                    }
                    int previousEnd = start - 1;
                    int previousStart = lineStart(previousEnd);
                    return move(Math.min(previousStart + column, previousEnd));
                }
                int end = lineEnd(cursorOffset);
                if (end == content.length()) {
                    return this;
                }
                int nextStart = end + 1;
                return move(Math.min(nextStart + column, lineEnd(nextStart)));
            }
            if (key.isConfirm()) {
                return insert("\n");
            }
            if (key.code() == dev.tamboui.tui.event.KeyCode.CHAR
                    && !key.hasCtrl() && !key.hasAlt()) {
                return insert(key.string());
            }
            return this;
        }

        Document navigate(KeyEvent key) {
            if (editing || pendingDiscard) {
                return this;
            }
            int lines = lineCount(content);
            int next = selectedLine;
            if (key.isUp()) {
                next = Math.max(0, selectedLine - 1);
            } else if (key.isDown()) {
                next = Math.min(lines - 1, selectedLine + 1);
            } else if (key.isHome()) {
                next = 0;
            } else if (key.isEnd()) {
                next = lines - 1;
            }
            return next == selectedLine
                    ? this
                    : new Document(
                            relativePath, originalContent, content, next,
                            lineStart(content, next), false, false);
        }

        private int lineStart(int offset) {
            int newline = content.lastIndexOf('\n', Math.max(0, offset - 1));
            return newline < 0 ? 0 : newline + 1;
        }

        private int lineEnd(int offset) {
            int newline = content.indexOf('\n', offset);
            return newline < 0 ? content.length() : newline;
        }

        private Document move(int offset) {
            return offset == cursorOffset
                    ? this
                    : new Document(
                            relativePath, originalContent, content, selectedLine,
                            offset, editing, pendingDiscard);
        }

        private Document with(String nextContent, int nextCursorOffset) {
            return new Document(
                    relativePath, originalContent, nextContent, selectedLine,
                    nextCursorOffset, editing, false);
        }

        private static boolean dirty(String original, String current) {
            return !current.equals(original);
        }

        private static int lineCount(String value) {
            int count = 1;
            for (int index = 0; index < value.length(); index++) {
                if (value.charAt(index) == '\n') {
                    count++;
                }
            }
            return count;
        }

        private static int lineOfOffset(String value, int offset) {
            int line = 0;
            for (int index = 0; index < offset; index++) {
                if (value.charAt(index) == '\n') {
                    line++;
                }
            }
            return line;
        }

        private static int lineStart(String value, int line) {
            if (line == 0) {
                return 0;
            }
            int current = 0;
            for (int index = 0; index < value.length(); index++) {
                if (value.charAt(index) == '\n' && ++current == line) {
                    return index + 1;
                }
            }
            return value.length();
        }

        private static int leadingSpaces(String value) {
            int count = 0;
            while (count < value.length() && value.charAt(count) == ' ') {
                count++;
            }
            return count;
        }

        private static boolean yamlScope(String trimmed) {
            if (trimmed.endsWith(":")) {
                return true;
            }
            if (!trimmed.startsWith("- ")) {
                return false;
            }
            return trimmed.substring(2).contains(":");
        }
    }
}
