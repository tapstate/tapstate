package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.ServerAddress;
import io.tapstate.core.model.SourceResource;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** A MongoDB destination that can be compared from catalog configuration without contacting it. */
public record MongoDestination(String database, String srvService, Set<String> hosts) {

    public static MongoDestination from(SourceResource source) {
        if (!Set.of("mongodb", "mongodb-atlas").contains(source.connector())) return null;
        var config = source.config();
        try {
            if (Boolean.FALSE.equals(config.get("isUri")) || "false".equals(config.get("isUri"))) {
                if (config.get("host") instanceof String host && config.get("database") instanceof String database) {
                    return from(new ConnectionString("mongodb://" + host.trim()), database);
                }
            } else if (config.get("uri") instanceof String uri) {
                // The driver normally resolves SRV TXT records while parsing. Catalog reads stay offline.
                var connection = new ConnectionString(uri, (name, type) -> List.of());
                return from(connection, connection.getDatabase());
            }
        } catch (IllegalArgumentException invalid) {
            // An unreadable destination still has its connection ID. Never include the URI in diagnostics.
        }
        return null;
    }

    private static MongoDestination from(ConnectionString connection, String database) {
        if (database == null || database.isBlank()) return null;
        boolean srv = connection.isSrvProtocol();
        Set<String> hosts = connection.getHosts().stream()
                .map(host -> srv ? host : new ServerAddress(host).toString())
                .map(host -> host.toLowerCase(Locale.ROOT))
                .collect(Collectors.toUnmodifiableSet());
        String srvService = srv ? Objects.requireNonNullElse(connection.getSrvServiceName(), "mongodb") : null;
        return new MongoDestination(database, srvService, hosts);
    }
}
