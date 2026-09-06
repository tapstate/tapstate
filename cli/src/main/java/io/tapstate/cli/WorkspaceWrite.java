package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Writes a set of files into a workspace - every one of them, or none.
 *
 * <p>Shared by {@code demo} and the recipes of the guided first run, which have the same promise to
 * keep: a workspace holding two of three files is a state nobody asked for and neither a re-run nor
 * {@code --force} was designed around. The existence check runs over the whole set before the first
 * byte is written; every directory is made before any file; under {@code --force} every target that
 * is already there is read first, so that a write that fails part way can put back what it replaced
 * and take back what it created.
 *
 * <p>A file marked {@link File#merged()} is one this invocation extends rather than owns - a
 * {@code .env} or a {@code .gitignore} that may already be the user's. It is exempt from the refusal
 * (the caller has already folded the existing content into what it hands over) but not from the
 * rollback.
 */
final class WorkspaceWrite {

    /**
     * One file to write, as a path relative to the workspace root and the exact text it will hold.
     *
     * @param merged whether an existing file at this path is extended rather than overwritten, and so
     *               does not trigger the refusal
     */
    record File(String path, String content, boolean merged) {
        static File owned(String path, String content) {
            return new File(path, content, false);
        }
    }

    /** One file after the write: where it landed, and whether something was there before. */
    record Written(Path path, boolean replaced) {}

    private WorkspaceWrite() {
    }

    /**
     * Writes {@code files} under {@code root}.
     *
     * @param force      whether an owned file that already exists may be replaced
     * @param existsCode the diagnostic to refuse with when one exists and it may not be; carries the
     *                   first such path as {@code path}
     * @return what was written, in the order given
     */
    static List<Written> write(Path root, List<File> files, boolean force, CliError existsCode) {
        if (!force) {
            for (File file : files) {
                Path target = root.resolve(file.path());
                if (!file.merged() && Files.exists(target)) {
                    throw new TapstateException(existsCode, Map.of("path", target.toString()), null);
                }
            }
        }
        // Every directory first, before any file. Creating one can fail on its own - a workspace holding
        // a plain file called `pipeline` passes the check above and fails here - and doing it up front
        // means that failure lands before the first byte rather than between two of them.
        try {
            Files.createDirectories(root);
        } catch (IOException cannotCreate) {
            throw notWritable(root, cannotCreate);
        }
        for (File file : files) {
            Path directory = root.resolve(file.path()).getParent();
            try {
                Files.createDirectories(directory);
            } catch (IOException cannotCreate) {
                throw notWritable(directory, cannotCreate);
            }
        }
        // A target may already hold something, and that something is the user's. Read it before
        // overwriting it, so the rollback below can put it back; a target that cannot be read is refused
        // here, while nothing has been written yet, rather than after it is already gone.
        List<Touched> touched = new ArrayList<>();
        for (File file : files) {
            Path target = root.resolve(file.path());
            String existing = null;
            if (Files.exists(target)) {
                try {
                    existing = Files.readString(target);
                } catch (IOException cannotRead) {
                    throw notWritable(target, cannotRead);
                }
            }
            touched.add(new Touched(target, existing));
        }
        List<Touched> done = new ArrayList<>();
        for (int i = 0; i < files.size(); i++) {
            Touched target = touched.get(i);
            try {
                Files.writeString(target.path(), files.get(i).content());
            } catch (IOException cannotWrite) {
                // All or none, kept as a promise rather than as an intention. What this invocation
                // wrote is taken back, so a reader is left with the workspace they had - which for the
                // ordinary case is no workspace at all, and never two files out of three. A file that
                // --force overwrote is put back with the bytes it held, because deleting it would make
                // this command destroy content on a path where it wrote nothing that survived.
                undo(done);
                throw notWritable(target.path(), cannotWrite);
            }
            done.add(target);
        }
        return done.stream().map(t -> new Written(t.path(), t.existing() != null)).toList();
    }

    /** A target this invocation is about to write, and what it held first - {@code null} if nothing. */
    private record Touched(Path path, String existing) {}

    /**
     * Puts back what this invocation changed: a file it created is removed, a file it overwrote is
     * restored. Best effort by necessity: it runs while a write has already failed, so the filesystem is
     * not answering, and a second failure here must not replace the first one in front of the reader.
     */
    private static void undo(List<Touched> done) {
        for (Touched target : done) {
            try {
                if (target.existing() == null) {
                    Files.deleteIfExists(target.path());
                } else {
                    Files.writeString(target.path(), target.existing());
                }
            } catch (IOException leaveIt) {
                // Reported through the diagnostic below, as the state the reader is actually in.
            }
        }
    }

    private static TapstateException notWritable(Path path, IOException failure) {
        return new TapstateException(
                CliError.WORKSPACE_NOT_WRITABLE,
                Map.of("path", path.toString(), "reason", reason(failure)), null);
    }

    /** What the filesystem said, in one line, for the diagnostic's named parameter. */
    private static String reason(IOException failure) {
        String message = failure.getMessage();
        return message == null || message.isBlank() ? failure.getClass().getSimpleName() : message;
    }
}
