package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.control.core.*;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageSetupStateStore;
import io.tapstate.spi.store.User;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Map;

/** Native CONFIG transaction witness only; the separate HTTP fixture proves management-channel authorization. */
public final class FirstAdministratorTransactionProcess {
    private FirstAdministratorTransactionProcess() {}
    public static void main(String[] args) throws Exception {
        var logger = (ch.qos.logback.classic.Logger)org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(ch.qos.logback.classic.Level.ERROR);
        var files = new InstallationFiles(Path.of(args[0]));
        try (var configuration = MongoConfigurationDomain.openExisting(OnPremInstallation.settings(files, new MongoProperties()))) {
            var scope = configuration.setupStates().load().scope();
            System.out.println("ADMIN_TRANSACTION_READY"); System.out.flush();
            var input = (Map<?,?>)JsonReader.parse(new String(System.in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
            String username = (String)input.get("username"), password = (String)input.get("password");
            try {
                var user = new User(username, new BCryptPasswordHasher().hash(password), "admin");
                new AuditGate(configuration.audit(), Clock.systemUTC()).dispatch(ControlOperations.USER_CREATE,
                        new AuditContext(username, scope.operationId()), () -> configuration.firstAdmin()
                                .guardedBy(() -> configuration.setupStates().requireCurrent(scope, StorageSetupStateStore.Purpose.FIRST_ADMIN))
                                .create(scope, user));
                System.out.println("ADMIN_TRANSACTION_COMMITTED");
            } catch (TapstateException refused) { System.out.println("ADMIN_TRANSACTION_REFUSED " + refused.code().code()); }
        }
    }
}
