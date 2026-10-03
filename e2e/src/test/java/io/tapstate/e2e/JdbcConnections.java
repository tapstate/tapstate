package io.tapstate.e2e;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;

/**
 * Opens the harness's own JDBC connections through the drivers its own class loader sees.
 *
 * <p>Not through {@code DriverManager}: it looks for the drivers on the classpath once per JVM, through the
 * thread context loader of whoever asks first. A server running in process asks from inside a connector,
 * under that connector's loader, which sees the connector's jar and nothing of the harness's classpath. A
 * driver nothing else ever loads is then never registered for the rest of the JVM -- Oracle's, whose
 * container waits on a log line rather than on a connection -- and a case asking for it fails with "no
 * suitable driver", or not, depending on which case the same JVM happened to run first.
 */
final class JdbcConnections {

    private static final List<Driver> DRIVERS = ServiceLoader.load(Driver.class, JdbcConnections.class.getClassLoader())
            .stream()
            .map(ServiceLoader.Provider::get)
            .toList();

    private JdbcConnections() {
    }

    static Connection open(String url, String user, String password) throws SQLException {
        Properties credentials = new Properties();
        credentials.setProperty("user", user);
        credentials.setProperty("password", password);
        for (Driver driver : DRIVERS) {
            if (driver.acceptsURL(url)) {
                return driver.connect(url, credentials);
            }
        }
        throw new SQLException("no driver on the harness's classpath accepts " + url);
    }
}
