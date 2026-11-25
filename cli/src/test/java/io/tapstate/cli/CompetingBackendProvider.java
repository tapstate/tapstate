package io.tapstate.cli;

import dev.tamboui.terminal.Backend;
import dev.tamboui.terminal.BackendProvider;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/** A discoverable backend that must never own the workbench terminal. */
public final class CompetingBackendProvider implements BackendProvider {

    static final AtomicBoolean CREATE_CALLED = new AtomicBoolean();

    @Override
    public String name() {
        return "competing-test-backend";
    }

    @Override
    public Backend create() throws IOException {
        CREATE_CALLED.set(true);
        throw new IOException("the competing test backend must not be selected");
    }
}
