package io.tapstate.core.catalog;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SecretConfigPathsTest {

    private static SecretConfigPaths.Path sshPassword() {
        return SecretConfigPaths.shared().stream()
                .filter(path -> "__TAPDATA.sshPassword".equals(path.name()))
                .findFirst()
                .orElseThrow();
    }

    @Test
    void removesExplicitlyNullNestedSecretsAndPreservesSiblings() {
        Map<String, Object> tapdata = new LinkedHashMap<>();
        tapdata.put("sshPassword", null);
        tapdata.put("sshHost", "bastion.example");
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("__TAPDATA", tapdata);

        sshPassword().remove(config);

        assertThat(config).containsEntry("__TAPDATA", Map.of("sshHost", "bastion.example"));
    }

    @Test
    void preservesEmptyNestedContainersWhenRemovingTheirOnlyNullSecret() {
        Map<String, Object> tapdata = new LinkedHashMap<>();
        tapdata.put("sshPassword", null);
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("__TAPDATA", tapdata);

        sshPassword().remove(config);

        assertThat(config).containsEntry("__TAPDATA", Map.of());
    }
}
