package io.tapstate.control.core;

import java.net.URI;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Objects;

/** Operator-owned model and transport. Workspace credentials never enter this route. */
public record ManagedAiProvider(AiProviderSettings settings, AiProviderClient client) {

    @FunctionalInterface
    public interface Signer {
        void sign(HttpRequest.Builder request, URI endpoint, byte[] json);
    }

    public static ManagedAiProvider bedrock(String region, String model, int requestsPerMinute, Signer signer) {
        if (!region.matches("[a-z]{2}(?:-[a-z0-9]+)+-[0-9]") || model.isBlank() || model.length() > 200
                || requestsPerMinute < 1) throw new IllegalArgumentException("Invalid managed AI configuration");
        URI base = URI.create("https://bedrock-runtime." + region + ".amazonaws.com/openai/v1");
        return new ManagedAiProvider(new AiProviderSettings("bedrock-managed", base.toString(), model, null),
                new OpenAiCompatibleClient(ignored -> base, Duration.ofSeconds(90),
                        Objects.requireNonNull(signer), requestsPerMinute));
    }
}
