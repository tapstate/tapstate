package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ConfigurationImportStore.*;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineLayout;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.User;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;

class ConfigurationImportPayloadTest {
    private static final Scope SCOPE = new Scope("cluster",1,StorageBinding.Mode.OP,null);
    private static SourceResource source(String id,Map<String,Object> config) { return new SourceResource(id,null,"mysql",config,null,null,null,null); }
    private static Payload payload(List<io.tapstate.core.model.Resource> sources,List<User> users) {
        return new Payload(sources,users,List.of(),Map.of(),Map.of());
    }
    private static Ticket ticket(ConfigurationImportPayload frozen) {
        Instant now = Instant.parse("2026-10-19T00:00:00Z");
        return new Ticket(UUID.randomUUID().toString(),SCOPE,frozen.inputHash,frozen.mappingHash,5,now,now.plusSeconds(1800));
    }
    @Test void nestedConnectionInputsAreCapturedBeforeAnyLaterGuardOrTransactionRetry() {
        var options = new LinkedHashMap<String,Object>(); var values = new ArrayList<>(List.of("original")); options.put("values",values);
        var settings = new LinkedHashMap<String,Object>(); settings.put("password","private-password"); settings.put("options",options);
        var original = payload(List.of(source("src",settings)),List.of(new User("new-user","private-user-hash","admin")));
        var frozen = new ConfigurationImportPayload(original); String hash = frozen.inputHash;
        settings.put("password","changed"); options.put("values",List.of("replaced")); values.clear(); values.add("changed");
        var saved = (SourceResource) frozen.payload.resources().getFirst();
        assertThat(saved.config()).containsEntry("password","private-password");
        assertThat(((Map<?,?>)saved.config().get("options")).get("values")).isEqualTo(List.of("original"));
        assertThat(frozen.inputHash).isEqualTo(hash);
        String receipt = JsonWriter.write(ConfigurationImportPayload.receiptTree(frozen.receipt(ticket(frozen))));
        assertThat(receipt).contains("new-user","src").doesNotContain("private-password","private-user-hash","original");
    }
    @Test void reorderingsKeepTheSameHashButMappingsAndUserCredentialsRemainPartOfTheInput() {
        var a = source("a",Map.of("host","a")); var b = source("b",Map.of("host","b"));
        var first = new ConfigurationImportPayload(payload(List.of(a,b),List.of(new User("u","hash-one","admin"))));
        var second = new ConfigurationImportPayload(payload(List.of(b,a),List.of(new User("u","hash-one","admin"))));
        assertThat(first.inputHash).isEqualTo(second.inputHash);
        assertThat(first.inputHash).isNotEqualTo(new ConfigurationImportPayload(payload(List.of(a,b),List.of(new User("u","hash-two","admin")))).inputHash);
        var mapped = new ConfigurationImportPayload(new Payload(List.of(a,b),first.payload.users(),List.of(),Map.of(),Map.of("old-target","views")));
        assertThat(mapped.inputHash).isNotEqualTo(first.inputHash); assertThat(mapped.mappingHash).isNotEqualTo(first.mappingHash);
    }
    @Test void duplicateResourcesAndUsersAndUnownedLayoutsAreRefusedWithoutIncludingTheRejectedContent() {
        assertThatThrownBy(() -> new ConfigurationImportPayload(payload(List.of(source("src",Map.of()),source("src",Map.of())),List.of())))
                .isInstanceOfSatisfying(TapstateException.class,failure -> assertThat(failure.code()).isEqualTo(IoError.CONFIGURATION_IMPORT_REFUSED));
        assertThatThrownBy(() -> new ConfigurationImportPayload(payload(List.of(),List.of(new User("u","secret-one","admin"),new User("u","secret-two","admin")))))
                .isInstanceOfSatisfying(TapstateException.class,failure -> assertThat(failure.args().toString()).doesNotContain("secret"));
        var layout = new PipelineLayout("absent",Map.of(),null);
        assertThatThrownBy(() -> new ConfigurationImportPayload(new Payload(List.of(source("src",Map.of())),List.of(),List.of(layout),Map.of(),Map.of())))
                .isInstanceOf(TapstateException.class);
        var pipeline = new PipelineResource("p",null,List.of(SourceRef.spec("src",true)),null,null,null,null,null);
        assertThat(new ConfigurationImportPayload(new Payload(List.of(pipeline,source("src",Map.of())),List.of(),
                List.of(new PipelineLayout("p",Map.of("src",new PipelineLayout.NodePosition(1,2)),null)),Map.of(),Map.of())).payload.layouts()).hasSize(1);
    }
    @Test void objectCountIndividualAndAggregateBytesHaveSeparateHardBudgets() {
        var many = new ArrayList<io.tapstate.core.model.Resource>();
        for (int i = 0; i < 65; i++) many.add(source("src"+i,Map.of()));
        assertLimit(() -> new ConfigurationImportPayload(payload(many,List.of())),"objects");
        assertLimit(() -> new ConfigurationImportPayload(payload(List.of(source("src",Map.of("large","x".repeat(1024*1024)))),List.of())),"object-bytes");
        var large = new ArrayList<io.tapstate.core.model.Resource>();
        for (int i = 0; i < 5; i++) large.add(source("src"+i,Map.of("large","x".repeat(900_000))));
        assertLimit(() -> new ConfigurationImportPayload(payload(large,List.of())),"payload-bytes");
    }
    @Test void aSmallInputStillCannotPublishAnOversizedReceipt() {
        var users = new ArrayList<User>(); for (int i = 0; i < 18; i++) users.add(new User("u"+i+"x".repeat(1000),"hash","admin"));
        var frozen = new ConfigurationImportPayload(payload(List.of(),users));
        assertLimit(() -> frozen.receipt(ticket(frozen)),"receipt-bytes");
    }
    private static void assertLimit(Runnable action,String limit) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class,failure -> {
            assertThat(failure.code()).isEqualTo(IoError.CONFIGURATION_IMPORT_LIMIT); assertThat(failure.args()).containsEntry("limit",limit);
        });
    }
}
