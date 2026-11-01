package io.tapstate.runtime.engine.nest;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

/** Measures the complete persistent state without allocating a second copy of its bytes. */
final class StateWaveSize {
    private StateWaveSize() { }

    static long of(Serializable value) {
        Counter counter = new Counter();
        try (var output = new ObjectOutputStream(counter)) {
            output.writeObject(value);
        } catch (IOException failure) {
            throw new IllegalStateException("cannot measure persistent state", failure);
        }
        return counter.bytes;
    }

    private static final class Counter extends OutputStream {
        private long bytes;
        @Override public void write(int value) { bytes++; }
        @Override public void write(byte[] values, int offset, int length) { bytes += length; }
    }
}
