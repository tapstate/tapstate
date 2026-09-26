package io.tapstate.messages;

import java.util.Map;

/** Shared display messages for retained event kinds and coded failures. */
public final class EventCatalog {

    private final MessageCatalog events;
    private final MessageCatalog failures;

    private EventCatalog(MessageCatalog events, MessageCatalog failures) {
        this.events = events;
        this.failures = failures;
    }

    public static EventCatalog bundled() {
        return new EventCatalog(MessageCatalog.fromResource("/messages/events-en.yml"),
                MessageCatalog.bundled());
    }

    public String render(String key, Map<String, Object> args) {
        MessageCatalog.Rendered event = events.render(key, args);
        return event.message().equals(key)
                ? failures.render(key, args).message()
                : event.message();
    }
}
