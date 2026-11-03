package io.tapstate.adapters.alerting;

import static org.assertj.core.api.Assertions.*;
import io.tapstate.core.alerting.*;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class AlarmTypeTextTest {
    @Test void allBuiltinKeysHaveFixedConditionsRecoveryAndGuidanceWithoutInventingAvailability() {
        var text = AlarmTypeText.english(); var catalog = BuiltinAlarmTypes.catalog();
        for (var type : catalog.types()) {
            var description = text.describe(type);
            assertThat(description.condition()).isNotBlank(); assertThat(description.recovery()).isNotBlank(); assertThat(description.guidance()).isNotBlank();
            assertThat(catalog.availability(type.id(), 1, 1, Set.of(), Set.of())).isEqualTo(AlarmTypeCatalog.Availability.INPUT_UNAVAILABLE);
        }
        var source = catalog.types().stream().filter(type -> type.id().equals("source.connection-failed")).findFirst().orElseThrow();
        assertThat(text.describe(source).recovery()).contains("same connection binding", "manual connection test is insufficient");
        var storage = catalog.types().stream().filter(type -> type.id().equals("cluster.storage-unavailable")).findFirst().orElseThrow();
        assertThat(text.describe(storage).guidance()).contains("stop new durable acceptance or send permits", "do not bypass the ledger");
    }
    @Test void eventDescriptionsExtendTheSameCatalogWithoutInventingRecoveryOrFallingBackToAnotherType() {
        var type = new AlarmTypeCatalog.Type("test.event", 1, AlarmTypeCatalog.Kind.EVENT, Set.of("pipeline"), "test-provider", "event", List.of(),
                AlarmSeverity.MINOR, false, "test.event.condition", null, "test.event.guidance");
        var text = new AlarmTypeText(Map.of("test.event.condition", "A fixed test event occurred.", "test.event.guidance", "Inspect its original event identity."));
        assertThat(text.describe(type).recovery()).isNull();
        assertThatThrownBy(() -> AlarmTypeText.english().describe(type)).isInstanceOf(IllegalStateException.class);
    }
}
