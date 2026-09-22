package io.tapstate.cli;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

/**
 * The seam through which the CLI runs another program: {@code docker compose}, and nothing else so far.
 * Everything that would start a process goes through here so that a unit test can script what the
 * program answers instead of needing it installed - the symmetric counterpart to {@link Prompter} on
 * the input side. The production runner starts a real process; the scripted one records and replies.
 */
interface ProcessRunner {

    /** What one run produced: its exit status and both of its streams, whole. */
    record Result(int exit, String stdout, String stderr) {
        boolean succeeded() {
            return exit == 0;
        }
    }

    /**
     * Runs {@code command} (the program and its arguments, one word each) in {@code dir} - or in this
     * process's own working directory when {@code dir} is null - and waits for it to finish. Throws only
     * when the program could not be started at all - a missing binary, a directory that does not exist;
     * a program that ran and failed is a {@link Result} with a non-zero exit. A probe whose answer does
     * not depend on where it runs passes null: a directory is a precondition, and one that the caller has
     * not created yet fails the run before the program is even looked up.
     */
    Result run(Path dir, List<String> command) throws IOException;

    /** The one that starts real processes. */
    static ProcessRunner system() {
        return (dir, command) -> {
            ProcessBuilder builder = new ProcessBuilder(command);
            if (dir != null) {
                builder.directory(dir.toFile());
            }
            Process process = builder.start();
            // stderr is drained on its own thread: a program that writes a lot there while stdout is
            // being read to the end would otherwise fill its pipe and block, and never exit
            byte[][] errBytes = new byte[1][];
            Thread drain = new Thread(() -> errBytes[0] = readAll(process.getErrorStream()), "process-stderr");
            drain.start();
            byte[] out = readAll(process.getInputStream());
            try {
                int exit = process.waitFor();
                drain.join();
                return new Result(exit, text(out), text(errBytes[0]));
            } catch (InterruptedException interrupted) {
                process.destroy();
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for " + command.get(0), interrupted);
            }
        };
    }

    private static byte[] readAll(InputStream stream) {
        try (stream) {
            return stream.readAllBytes();
        } catch (IOException unreadable) {
            return new byte[0];
        }
    }

    private static String text(byte[] bytes) {
        return bytes == null ? "" : new String(bytes, StandardCharsets.UTF_8);
    }
}
