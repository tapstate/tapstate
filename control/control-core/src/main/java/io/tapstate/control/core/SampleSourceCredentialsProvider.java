package io.tapstate.control.core;

/** Supplies server-only credentials for the shared demonstration databases. */
public interface SampleSourceCredentialsProvider {
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
}
