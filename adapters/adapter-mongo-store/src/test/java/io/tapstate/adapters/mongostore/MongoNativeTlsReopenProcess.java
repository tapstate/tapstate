package io.tapstate.adapters.mongostore;

import java.time.Duration;

/** New-process probe receives CONFIG inputs only; CACHE trust must come from the saved binding. */
public final class MongoNativeTlsReopenProcess {
    private MongoNativeTlsReopenProcess() {}
    public static void main(String[] arguments) {
        var settings = new MongoConnectionSettings(arguments[0], arguments[1], Duration.ofSeconds(5));
        try (var configuration = MongoConfigurationDomain.openExisting(settings)) {
            var binding = configuration.bindings().load().orElseThrow();
            var preparation = configuration.bindings().cachePreparation().orElseThrow();
            try (var cache = MongoCacheDomain.connect(binding.cache(), Duration.ofSeconds(5), "new-tls-process")) {
                cache.loadAuthScope(preparation.authScope());
                cache.verifyBusinessReady(preparation.businessScope());
                System.out.println("TLS_REOPEN_OK " + preparation.authScope().cacheIncarnation());
            }
        }
    }
}
