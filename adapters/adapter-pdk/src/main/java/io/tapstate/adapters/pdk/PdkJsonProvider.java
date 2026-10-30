package io.tapstate.adapters.pdk;

import io.tapdata.entity.utils.ClassFactory;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import java.util.Objects;
import java.util.function.Supplier;

/** Completes the shared PDK factory before concurrent connector work can enter it. */
final class PdkJsonProvider {
    private static final PdkJsonProvider SHARED = new PdkJsonProvider(
            () -> InstanceFactory.instance(JsonParser.class), ClassFactory.class);

    private final Supplier<JsonParser> initializer;
    private final Object monitor;
    private JsonParser parser;

    PdkJsonProvider(Supplier<JsonParser> initializer, Object monitor) {
        this.initializer = Objects.requireNonNull(initializer);
        this.monitor = Objects.requireNonNull(monitor);
    }

    static JsonParser parser() {
        return SHARED.get();
    }

    static void prepareRuntime() {
        try {
            Class.forName("io.tapdata.pdk.core.runtime.TapRuntime", false, PdkJsonProvider.class.getClassLoader());
        } catch (ClassNotFoundException absent) {
            // Synthetic-only hosts can open connectors without the optional PDK runtime.
            return;
        }
        parser();
    }

    JsonParser get() {
        synchronized (monitor) {
            // The PDK publishes its factory reference before its reflection methods. Use its
            // initialization monitor, and do not let plugin constructors run until it is complete.
            if (parser == null) parser = Objects.requireNonNull(initializer.get(), "PDK JSON provider");
            return parser;
        }
    }
}
