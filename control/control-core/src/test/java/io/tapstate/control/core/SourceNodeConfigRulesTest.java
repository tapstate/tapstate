package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SourceNodeConfigRulesTest {

    private static final SourceResource SOURCE = new SourceResource("orders", null, "mongo",
            Map.of("preImage", true), null, null, null, null);
    private static final ConnectorDetail CONNECTOR = new ConnectorDetail("mongo", "MongoDB", "", "database",
            List.of(), "", false, false, "", List.of(), SpecSource.of("hash", """
                    {
                      "configOptions": {
                        "connection": {"properties": {"uri": {"type": "string"}}},
                        "node": {"properties": {"advanced": {"properties": {
                          "preImage": {"type": "boolean"},
                          "callbackToken": {"type": "string", "writeOnly": true}
                        }}}}
                      }
                    }
                    """), true);

    @Test
    void admitsAnExplicitFalseForANestedNodeField() {
        assertThatCode(() -> SourceNodeConfigRules.validate(SOURCE, Map.of("preImage", false), id -> CONNECTOR))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsConnectionFieldsAndWriteOnlyNodeFields() {
        assertThatThrownBy(() -> SourceNodeConfigRules.validate(SOURCE, Map.of("uri", "value"), id -> CONNECTOR))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("undeclared field");
        assertThatThrownBy(() -> SourceNodeConfigRules.validate(SOURCE,
                Map.of("callbackToken", "value"), id -> CONNECTOR))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("cannot include connector credentials");
    }
}
