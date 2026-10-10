package io.tapstate.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.SampleSourceCredentialsProvider;
import io.tapstate.core.common.TapstateException;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Requests shared sample credentials from Global Control Plane server-to-server. */
final class CloudSampleSourceCredentialsProvider implements SampleSourceCredentialsProvider {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);

    private final CloudRuntimeSettings settings;
    private final HttpClient http;
    private final ObjectMapper objectMapper;

    CloudSampleSourceCredentialsProvider(CloudRuntimeSettings settings, ObjectMapper objectMapper) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    }

    @Override
    public boolean available() {
        try {
            HttpResponse<String> response = send("availability", "GET");
            if (response.statusCode() < 200 || response.statusCode() >= 300) return false;
            JsonNode data = data(response.body());
            return data.path("available").asBoolean(false);
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (RuntimeException e) {
            return false;
        }
    }

    @Override
    public Credentials fetch() {
        try {
            HttpResponse<String> response = send("credentials", "POST");
            if (response.statusCode() < 200 || response.statusCode() >= 300) throw unavailable();
            JsonNode envelope = objectMapper.readTree(response.body());
            if (!"ok".equals(envelope.path("code").asText())) throw unavailable();
            JsonNode data = envelope.path("data");
            String host = data.path("host").asText();
            String password = data.path("password").asText();
            if (host.isBlank() || password.isBlank()) throw unavailable();
            return new Credentials(host, password);
        } catch (IOException e) {
            throw unavailable();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw unavailable();
        } catch (RuntimeException e) {
            if (e instanceof TapstateException tapstateException) throw tapstateException;
            throw unavailable();
        }
    }

    @Override
    public List<Definition> catalog() {
        try {
            HttpResponse<String> response = send("catalog", "GET");
            if (response.statusCode() < 200 || response.statusCode() >= 300) return List.of();
            JsonNode entries = data(response.body());
            if (!entries.isArray()) return List.of();
            List<Definition> definitions = new ArrayList<>();
            boolean configured = available();
            for (JsonNode entry : entries) {
                definitions.add(new Definition(entry.path("id").asText(), entry.path("name").asText(),
                        entry.path("description").asText(), entry.path("connector").asText(),
                        entry.path("available").asBoolean(false) && configured,
                        entry.path("guidedDemo").asBoolean(false), entry.path("rootTable").asText(null),
                        entry.path("orderLineTable").asText(null), entry.path("customerTable").asText(null)));
            }
            return List.copyOf(definitions);
        } catch (IOException e) {
            return List.of();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        }
    }

    @Override
    public Map<String, Object> settingsFor(String id) {
        try {
            String sourceId = URLEncoder.encode(id, StandardCharsets.UTF_8);
            HttpResponse<String> response = send("credentials/" + sourceId, "POST");
            if (response.statusCode() < 200 || response.statusCode() >= 300) throw unavailable();
            JsonNode data = data(response.body());
            if (!data.isObject() || data.path("password").asText().isBlank()) throw unavailable();
            Map<String, Object> config = new LinkedHashMap<>();
            data.fields().forEachRemaining(field -> {
                if (field.getValue().isNumber()) config.put(field.getKey(), field.getValue().intValue());
                else config.put(field.getKey(), field.getValue().asText());
            });
            if ("postgres".equals(catalog().stream().filter(item -> item.id().equals(id))
                    .map(Definition::connector).findFirst().orElse(""))) {
                config.put("user", config.remove("username"));
                config.put("globalPublicationName", "tapstate_sample_pub");
            }
            return config;
        } catch (IOException e) {
            throw unavailable();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw unavailable();
        }
    }

    private HttpResponse<String> send(String endpoint, String method) throws IOException, InterruptedException {
        String clusterId = URLEncoder.encode(settings.clusterId(), StandardCharsets.UTF_8);
        URI uri = settings.baseUrl().resolve(
                "/v1/api/clusters/" + clusterId + "/sample-sources/" + endpoint);
        HttpRequest.Builder request = HttpRequest.newBuilder(uri)
                .timeout(REQUEST_TIMEOUT)
                .header("Accept", "application/json")
                .header("Authorization", "Bearer " + settings.token())
                .header("Cache-Control", "no-store");
        if ("POST".equals(method)) {
            request.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.noBody());
        } else {
            request.GET();
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private JsonNode data(String body) throws IOException {
        JsonNode envelope = objectMapper.readTree(body);
        if (!"ok".equals(envelope.path("code").asText())) return objectMapper.createObjectNode();
        return envelope.path("data");
    }

    private TapstateException unavailable() {
        return new TapstateException(ControlError.UNREACHABLE, Map.of(), null);
    }
}
