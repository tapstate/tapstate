package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/** Bounded client-owner-local admission; database fences still arbitrate all other owners. */
final class MongoPipelineWrites {
    private final ReentrantLock[] stripes = new ReentrantLock[64];

    MongoPipelineWrites() {
        for (int i = 0; i < stripes.length; i++) stripes[i] = new ReentrantLock(true);
    }

    <T> T call(String pipeline, Runnable requireActive, Supplier<T> operation) {
        int hash = pipeline.hashCode();
        ReentrantLock gate = stripes[(hash ^ (hash >>> 16)) & (stripes.length - 1)];
        requireActive.run();
        try {
            while (!gate.tryLock(100, TimeUnit.MILLISECONDS)) requireActive.run();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            requireActive.run();
            throw new TapstateException(IoError.STORE_UNAVAILABLE,
                    Map.of("detail", "interrupted waiting for a pipeline state writer"), null);
        }
        try {
            requireActive.run();
            return operation.get();
        } finally {
            gate.unlock();
        }
    }
}
