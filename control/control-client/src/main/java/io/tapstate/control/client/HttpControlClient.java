package io.tapstate.control.client;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Framework-free JDK HTTP transport shared by local control-plane frontends. */
public final class HttpControlClient implements AutoCloseable {

    public static final Duration DEFAULT_LIGHT_TIMEOUT = Duration.ofSeconds(3);
    public static final Duration DEFAULT_HEAVY_TIMEOUT = Duration.ofSeconds(30);

    private final Duration lightTimeout;
    private final Duration heavyTimeout;
    private final ExecutorService executor;
    private final HttpClient client;
    private final Set<CompletableFuture<?>> inFlight = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    public HttpControlClient() {
        this(DEFAULT_LIGHT_TIMEOUT, DEFAULT_HEAVY_TIMEOUT);
    }

    public HttpControlClient(Duration lightTimeout, Duration heavyTimeout) {
        this.lightTimeout = requirePositive(lightTimeout, "lightTimeout");
        this.heavyTimeout = requirePositive(heavyTimeout, "heavyTimeout");
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.client = HttpClient.newBuilder()
                .connectTimeout(this.lightTimeout)
                .executor(executor)
                .build();
    }

    public ControlResponse get(URI baseUrl, String token, String path) {
        return exchange(baseUrl, token, path, "GET", null, null, RequestBudget.LIGHT);
    }

    public ControlResponse post(
            URI baseUrl, String token, String path, Object body, RequestBudget budget) {
        HttpRequest.BodyPublisher publisher = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(JsonWriter.write(body), StandardCharsets.UTF_8);
        return exchange(baseUrl, token, path, "POST", publisher, null, budget);
    }

    /**
     * Removes the resource at {@code path}, carrying {@code expectedContentHash} as a quoted entity tag
     * so the server can refuse a caller holding a stale version. A null hash sends no {@code If-Match} at
     * all rather than an empty one: "I supplied no precondition" and "I supplied a malformed one" are
     * different refusals, and the second would misreport the first.
     */
    public ControlResponse delete(URI baseUrl, String token, String path, String expectedContentHash) {
        String ifMatch = expectedContentHash == null ? null : "\"" + expectedContentHash + "\"";
        return exchange(baseUrl, token, path, "DELETE", null, ifMatch, RequestBudget.LIGHT);
    }

    private ControlResponse exchange(
            URI baseUrl,
            String token,
            String path,
            String method,
            HttpRequest.BodyPublisher body,
            String ifMatch,
            RequestBudget budget) {
        Objects.requireNonNull(baseUrl, "baseUrl");
        Objects.requireNonNull(token, "token");
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(budget, "budget");
        if (closed.get()) {
            return new ControlResponse.Unreachable();
        }
        try {
            HttpRequest.Builder request = HttpRequest.newBuilder(endpoint(baseUrl, path))
                    .header("Authorization", "Bearer " + token)
                    .header("Accept", "application/json");
            budget.timeout(lightTimeout, heavyTimeout).ifPresent(request::timeout);
            if (ifMatch != null) {
                request.header("If-Match", ifMatch);
            }
            if (body == null) {
                request.method(method, HttpRequest.BodyPublishers.noBody());
            } else {
                request.header("Content-Type", "application/json").method(method, body);
            }
            return decode(send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)));
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            return new ControlResponse.Unreachable();
        } catch (IOException | RuntimeException error) {
            return new ControlResponse.Unreachable();
        }
    }

    /**
     * Sends a request with its declared deadline, or waits for completion when none is declared.
     * Connection establishment remains bounded; interruption and close cancel the exchange.
     */
    public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler)
            throws IOException, InterruptedException {
        CompletableFuture<HttpResponse<T>> future = client.sendAsync(request, handler);
        inFlight.add(future);
        if (closed.get()) {
            future.cancel(true);
        }
        Optional<Duration> timeout = request.timeout();
        try {
            return timeout.isPresent()
                    ? future.get(Math.max(1, timeout.get().toMillis()), TimeUnit.MILLISECONDS)
                    : future.get();
        } catch (InterruptedException error) {
            future.cancel(true);
            throw error;
        } catch (TimeoutException error) {
            future.cancel(true);
            throw new HttpTimeoutException("HTTP request exceeded " + timeout.orElseThrow());
        } catch (ExecutionException error) {
            Throwable cause = error.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error fatal) {
                throw fatal;
            }
            throw new IOException("HTTP request failed", cause);
        } finally {
            inFlight.remove(future);
        }
    }

    private ControlResponse decode(HttpResponse<String> response) {
        int status = response.statusCode();
        Object body;
        try {
            body = response.body() == null || response.body().isBlank()
                    ? null
                    : JsonReader.parse(response.body());
        } catch (RuntimeException malformed) {
            if (status >= 200 && status < 300) {
                return new ControlResponse.Unreachable();
            }
            body = null;
        }
        if (status >= 200 && status < 300) {
            return new ControlResponse.Success(status, body);
        }
        Map<?, ?> object = body instanceof Map<?, ?> map ? map : Map.of();
        String code = object.get("code") instanceof String value ? value : "";
        String message = object.get("message") instanceof String value
                ? value
                : "The server refused the request.";
        return new ControlResponse.Rejected(status, code, message, params(object.get("params")));
    }

    private static URI endpoint(URI baseUrl, String path) {
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("control path must start with '/': " + path);
        }
        String base = baseUrl.toString();
        return URI.create((base.endsWith("/") ? base.substring(0, base.length() - 1) : base) + path);
    }

    private static Map<String, Object> params(Object value) {
        if (!(value instanceof Map<?, ?> map)) {
            return Map.of();
        }
        Map<String, Object> out = new LinkedHashMap<>();
        map.forEach((key, item) -> out.put(String.valueOf(key), item));
        return out;
    }

    private static Duration requirePositive(Duration duration, String name) {
        Objects.requireNonNull(duration, name);
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return duration;
    }

    /** Cancels in-flight exchanges and releases the client executor. */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            inFlight.forEach(future -> future.cancel(true));
            client.shutdownNow();
            executor.shutdownNow();
        }
    }
}
