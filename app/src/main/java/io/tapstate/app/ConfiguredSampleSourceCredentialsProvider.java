package io.tapstate.app;

import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.SampleSourceCredentialsProvider;
import io.tapstate.core.common.TapstateException;
import java.util.Map;

/** Uses server-side configuration for on-prem demonstration databases. */
final class ConfiguredSampleSourceCredentialsProvider implements SampleSourceCredentialsProvider {
    private final String host;
    private final String password;

    ConfiguredSampleSourceCredentialsProvider(String host, String password) {
        this.host = host;
        this.password = password;
    }

    @Override
    public boolean available() {
        return password != null && !password.isBlank();
    }

    @Override
    public Credentials fetch() {
        if (!available()) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "sample database credentials are not configured on the server"), null);
        }
        return new Credentials(host, password);
    }
}
