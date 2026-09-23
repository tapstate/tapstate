package io.tapstate.messages;

import java.util.Map;

/** Shared English wording for explain messages, limitations and next actions. */
public final class ExplanationCatalog {

    private final MessageCatalog explanations;
    private final MessageCatalog errors;

    private ExplanationCatalog(MessageCatalog explanations, MessageCatalog errors) {
        this.explanations = explanations;
        this.errors = errors;
    }

    public static ExplanationCatalog bundled() {
        return new ExplanationCatalog(
                MessageCatalog.fromResource("/messages/explain-en.yml"), MessageCatalog.bundled());
    }

    /** Renders an explain key, falling back to the error catalog for a carried failure code. */
    public String render(String key, Map<String, Object> args) {
        MessageCatalog.Rendered rendered = explanations.render(key, args);
        if (!rendered.message().equals(key)) {
            return rendered.message();
        }
        return errors.render(key, args).message();
    }
}
