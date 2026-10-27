package io.tapstate.core.logging;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MongoLogSanitizerTest {

    @Test
    void hidesDriverAcceptedQuotesAndWhitespaceInUserInfo() {
        SecretRedactor redactor = new SecretRedactor();
        assertThat(redactor.redactLog("connecting mongodb://quote-user:pa\"ss@host.example/db"))
                .isEqualTo("connecting mongodb://<redacted>@host.example/db");
        assertThat(redactor.redactLog("connecting mongodb://space-user:pa ss@host.example/db"))
                .isEqualTo("connecting mongodb://<redacted>@host.example/db");
    }

    @Test
    void longUriDiagnosticsDoNotDependOnRegexStackDepth() {
        String path = "x".repeat(100_000);
        assertThat(new SecretRedactor().redactLog("mongodb://user:password@host.example/" + path))
                .isEqualTo("mongodb://<redacted>@host.example/" + path);
    }

    @Test
    void redactsEachAdjacentUriInCompactStructuredDiagnostics() {
        String text = "{\"source\":\"mongodb://u1:p1@host1/db\","
                + "\"target\":\"mongodb://u2:p2@host2/db\"}";

        assertThat(new SecretRedactor().redactLog(text))
                .isEqualTo("{\"source\":\"mongodb://<redacted>@host1/db\","
                        + "\"target\":\"mongodb://<redacted>@host2/db\"}");
    }

    @Test
    void hidesDriverCredentialsBeforeRegistrationAndPreservesTheEndpoint() {
        SecretRedactor redactor = new SecretRedactor();
        String text = "settings credential=MongoCredential{mechanism=SCRAM-SHA-256, "
                + "userName='atlas-user', source='private-auth', password=<hidden>, mechanismProperties=<hidden>}, "
                + "hosts=[db.example:27017]";

        assertThat(redactor.redactLog(text))
                .isEqualTo("settings credential=MongoCredential<redacted>, hosts=[db.example:27017]");
    }

    @Test
    void handlesQuotesBracesMultipleCredentialsAndIncompleteDumpsConservatively() {
        SecretRedactor redactor = new SecretRedactor();
        String text = "MongoCredential{userName='a'}b', mechanismProperties=<hidden>} "
                + "cause MongoCredential{userName='second', mechanismProperties=<hidden>} endpoint=db.example";

        assertThat(redactor.redactLog(text))
                .isEqualTo("MongoCredential<redacted> endpoint=db.example");
        assertThat(redactor.redactLog("failed MongoCredential{userName='private'\npartial settings"))
                .isEqualTo("failed MongoCredential<redacted>");
    }

    @Test
    void hidesUriAndProxyCredentialsWithNoRegisteredSource() {
        SecretRedactor redactor = new SecretRedactor();
        String text = "mongodb+srv://a%40b:p%40ss@db.example/data?tls=true "
                + "ProxySettings{host=proxy, port=1080, username=private, password=secret}";

        assertThat(redactor.redactLog(text))
                .isEqualTo("mongodb+srv://<redacted>@db.example/data?tls=true ProxySettings<redacted>");
    }

    @Test
    void repeatedLogRedactionKeepsMarkersAndRegisteredShortSecretsIntact() {
        SecretRedactor redactor = new SecretRedactor();
        redactor.replace("source", java.util.List.of("ed", "known-password"));
        String text = "MongoCredential{userName='private', mechanismProperties=<hidden>} "
                + "mongodb://a:secret@db.example/data ed known-password unrelated";

        String safe = redactor.redactLog(text);

        assertThat(safe).endsWith(" ******** ******** unrelated");
        assertThat(redactor.redactLog(safe)).isEqualTo(safe);
        assertThat(redactor.redact("mongodb://a:secret@db.example/data"))
                .isEqualTo("mongodb://a:secret@db.example/data");
    }
}
