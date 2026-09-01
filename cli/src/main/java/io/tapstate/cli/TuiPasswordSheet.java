package io.tapstate.cli;

import java.util.List;

/** Render-safe login sheet metadata paired with a private, wipeable password buffer. */
final class TuiPasswordSheet implements AutoCloseable {

    private final String server;
    private final String issuer;
    private final String username;
    private final TuiSecretInput password = new TuiSecretInput();

    TuiPasswordSheet(String server, String issuer, String username) {
        this.server = displayValue(server);
        this.issuer = displayValue(issuer);
        this.username = displayValue(username);
    }

    void appendCodePoint(int codePoint) {
        password.appendCodePoint(codePoint);
    }

    void deleteLastCodePoint() {
        password.deleteLastCodePoint();
    }

    String maskedPassword() {
        return password.masked();
    }

    List<String> frame() {
        return List.of(
                "Server: " + server,
                "Issuer: " + issuer,
                "Username: " + username,
                "Password: " + password.masked());
    }

    String submit() {
        return password.take();
    }

    @Override
    public void close() {
        password.close();
    }

    @Override
    public String toString() {
        return "TuiPasswordSheet[server=" + server + ", issuer=" + issuer
                + ", username=" + username + ", password=" + password + ']';
    }

    private static String displayValue(String value) {
        return value == null || value.isBlank() ? "(unknown)" : value;
    }
}
