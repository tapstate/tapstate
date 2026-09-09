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
                Focus.FILES, Optional.of(Document.open(relativePath, content)));
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
                Focus.VIEWER, Optional.of(document.orElseThrow().edit(key)));
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
            int cursorOffset,
            boolean editing) {

        Document {
            Objects.requireNonNull(relativePath, "relativePath");
            Objects.requireNonNull(originalContent, "originalContent");
            Objects.requireNonNull(content, "content");
            if (relativePath.isAbsolute() || relativePath.normalize().startsWith("..")) {
                throw new IllegalArgumentException("Workspace documents require a relative path");
            }
            if (cursorOffset < 0 || cursorOffset > content.length()) {
                throw new IllegalArgumentException("Cursor is outside the document");
            }
        }

        static Document open(Path relativePath, String content) {
            return new Document(relativePath.normalize(), content, content, 0, false);
        }

        Document edit() {
            return editing ? this : new Document(relativePath, content, content, 0, true);
        }

        Document cancelEdit() {
            return new Document(relativePath, originalContent, originalContent, 0, false);
        }

        Document saved(boolean closeEditor) {
            return new Document(relativePath, content, content, cursorOffset, !closeEditor);
        }

        boolean dirty() {
            return !content.equals(originalContent);
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
                    : new Document(relativePath, originalContent, content, offset, editing);
        }

        private Document with(String nextContent, int nextCursorOffset) {
            return new Document(relativePath, originalContent, nextContent, nextCursorOffset, editing);
        }
    }
}
