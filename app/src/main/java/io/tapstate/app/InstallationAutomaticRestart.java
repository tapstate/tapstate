package io.tapstate.app;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntConsumer;

/** A single successful initial handoff may ask the installation launcher for a fresh process. */
final class InstallationAutomaticRestart implements Runnable {
    static final int EXIT_RESTART = 75;
    private final AtomicBoolean requested = new AtomicBoolean();
    private final Runnable close;
    private final IntConsumer exit;
    InstallationAutomaticRestart(Runnable close, IntConsumer exit) { this.close = close; this.exit = exit; }
    @Override public void run() {
        if (!requested.compareAndSet(false, true)) return;
        Thread.ofPlatform().name("installation-restart").start(() -> {
            try {
                Thread.sleep(2000);
                close.run();
                exit.accept(EXIT_RESTART);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException | Error failure) {
                org.slf4j.LoggerFactory.getLogger(InstallationAutomaticRestart.class).error("Installation restart could not release the original runtime", failure);
                exit.accept(1);
            }
        });
    }
}
