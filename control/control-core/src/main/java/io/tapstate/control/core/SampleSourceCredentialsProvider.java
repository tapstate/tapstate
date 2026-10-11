package io.tapstate.control.core;

import java.util.List;
import java.util.Map;

/** Supplies server-only credentials for the shared demonstration databases. */
public interface SampleSourceCredentialsProvider {
    record Definition(String id, String name, String description, String connector,
                      boolean available, boolean guidedDemo, String rootTable,
                      String orderLineTable, String customerTable) { }

    record Credentials(String host, String password) {
        public Credentials {
            if (host == null || host.isBlank()) throw new IllegalArgumentException("host is required");
            if (password == null || password.isBlank()) throw new IllegalArgumentException("password is required");
        }

        @Override
        public String toString() {
            return "Credentials[host=" + host + ", password=<redacted>]";
        }
    }

    boolean available();

    Credentials fetch();

    default List<Definition> catalog() {
        if (!available()) return List.of();
        return List.of(
                new Definition("sample_core_banking", "MySQL order sample",
                        "Orders with customer and order lines", "mysql", true, true,
                        "bmsql_oorder", "bmsql_order_line", "bmsql_customer"),
                new Definition("sample_cards_crm", "PostgreSQL order sample",
                        "Independent order dataset", "postgres", true, true,
                        "T_9_3_2_bmsql_oorder", "T_9_3_2_bmsql_order_line",
                        "T_9_3_2_bmsql_customer"));
    }

    default Map<String, Object> settingsFor(String id) {
        Credentials credentials = fetch();
        if ("sample_core_banking".equals(id)) {
            return Map.of("host", credentials.host(), "port", 33306,
                    "database", "tpcc_331", "username", "root", "password", credentials.password());
        }
        if ("sample_cards_crm".equals(id)) {
            return Map.of("host", credentials.host(), "port", 55433,
                    "database", "postgres", "schema", "public", "user", "root",
                    "password", credentials.password(), "globalPublicationName", "tapstate_sample_pub");
        }
        throw new IllegalArgumentException("unknown sample source id");
    }
}
