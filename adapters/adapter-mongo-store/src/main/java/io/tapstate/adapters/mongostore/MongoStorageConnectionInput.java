package io.tapstate.adapters.mongostore;

import com.mongodb.AuthenticationMechanism;
import com.mongodb.ConnectionString;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageBinding;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Normalizes one storage candidate without changing URI seed, SRV/TXT or authentication semantics. */
public final class MongoStorageConnectionInput {
    private MongoStorageConnectionInput() {}

    public static StorageBinding.Connection normalize(StorageBinding.Connection input) {
        Objects.requireNonNull(input, "input");
        String uri = input.uri();
        try {
            if (!(uri.startsWith("mongodb://") || uri.startsWith("mongodb+srv://")) || uri.indexOf('#') >= 0) throw refused();
            Map<String, String> options = options(uri);
            if (options.containsKey("tls") && options.containsKey("ssl")
                    && !options.get("tls").equalsIgnoreCase(options.get("ssl"))) throw refused();
            if (input.kind() == StorageBinding.Connection.Kind.ATLAS) {
                if (options.containsKey("authsource") && !"admin".equals(options.get("authsource"))) throw refused();
                if (options.containsKey("authmechanism")
                        && !java.util.Set.of("SCRAM-SHA-256", "SCRAM-SHA-1").contains(options.get("authmechanism"))) throw refused();
                if ((options.containsKey("tls") && !"true".equalsIgnoreCase(options.get("tls")))
                        || (options.containsKey("ssl") && !"true".equalsIgnoreCase(options.get("ssl")))) throw refused();
                if (!options.containsKey("authsource")) uri = append(uri, "authSource=admin");
                if (!options.containsKey("tls") && !options.containsKey("ssl")) uri = append(uri, "tls=true");
            }
            StorageBinding.Connection normalized = new StorageBinding.Connection(input.kind(), uri, input.database(), input.caPem());
            try (MongoConnection connection = MongoConnection.forCache(normalized, Duration.ofSeconds(10), null)) {
                ConnectionString parsed = MongoConnection.parseConnectionString(uri, ConnectionString::new);
                connection.selectedDatabase(parsed);
                connection.buildClientSettings(parsed);
                if (input.kind() == StorageBinding.Connection.Kind.ATLAS
                        && (parsed.getUsername() == null || parsed.getPassword() == null
                        || (parsed.getCredential().getAuthenticationMechanism() != null
                        && parsed.getCredential().getAuthenticationMechanism() != AuthenticationMechanism.SCRAM_SHA_256
                        && parsed.getCredential().getAuthenticationMechanism() != AuthenticationMechanism.SCRAM_SHA_1))) throw refused();
            }
            return normalized;
        } catch (IllegalArgumentException invalid) {
            throw refused();
        }
    }

    /** Freezes the effective original authSource before changing a logical target database. */
    public static String databaseUri(StorageBinding.Connection normalized) {
        String uri = normalized.uri();
        ConnectionString parsed = MongoConnection.parseConnectionString(uri, ConnectionString::new);
        if (!options(uri).containsKey("authsource") && parsed.getCredential() != null) {
            uri = append(uri, "authSource=" + java.net.URLEncoder.encode(parsed.getCredential().getSource(), StandardCharsets.UTF_8));
        }
        int start = uri.indexOf("://") + 3;
        int query = uri.indexOf('?', start);
        int end = query < 0 ? uri.length() : query;
        int slash = uri.indexOf('/', start);
        if (slash < 0 || slash > end) slash = end;
        return uri.substring(0, slash) + "/" + java.net.URLEncoder.encode(normalized.database(), StandardCharsets.UTF_8) + uri.substring(end);
    }

    private static Map<String, String> options(String uri) {
        Map<String, String> result = new HashMap<>();
        int query = uri.indexOf('?');
        if (query < 0) return result;
        for (String entry : uri.substring(query + 1).split("[&;]", -1)) {
            String[] pair = entry.split("=", 2);
            if (pair.length != 2 || pair[0].isEmpty()) throw refused();
            String key = URLDecoder.decode(pair[0], StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
            if (result.putIfAbsent(key, URLDecoder.decode(pair[1], StandardCharsets.UTF_8)) != null) throw refused();
        }
        return result;
    }

    private static String append(String uri, String option) {
        int query = uri.indexOf('?');
        int end = query < 0 ? uri.length() : query;
        int slash = uri.indexOf('/', uri.indexOf("://") + 3);
        if (slash < 0 || slash > end) uri = uri.substring(0, end) + "/" + uri.substring(end);
        return uri + (query < 0 ? "?" : "&") + option;
    }

    private static TapstateException refused() {
        return new TapstateException(StoreError.STORAGE_CONNECTION_UNSUPPORTED, Map.of(), null);
    }
}
