package io.tapstate.control.core;

/** Ephemeral provider input; its textual diagnostic representation never contains credentials. */
public record AiProviderSettings(String providerId, String baseUrl, String model, String apiKey) {
    @Override public String toString() { return "AiProviderSettings[redacted]"; }
}
