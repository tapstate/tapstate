package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlertError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import io.tapstate.spi.store.AlertNodeLifecycleStore.*;
import java.time.Instant;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MongoAlertNodeLifecycleCodecTest {
    static final Instant T=Instant.parse("2026-10-29T00:00:00Z");
    static Registration registration() {
        var scope=new AlarmKey.Scope("installation-a","cluster-a");var key=new Key(scope,"node-a","boot-a");
        return new Registration(key,new StorageSetupScope("cluster-a","installation-a","setup-a",1),
                new CacheAuthScope("cluster-a",1,StorageBinding.Mode.OP,"cache-a",1),
                new WorkloadClaimFence(new WorkloadClaimKey("cluster-a",WorkloadClaimType.NODE_SESSION,"node-a"),new WorkloadOwner("node-a","boot-a"),1,0,1),
                "e6e8d616-985e-49cc-a0f4-9802897abf95",1);
    }
    @Test void anExpectedBootHasOneExplicitBoundedFormatIndependentOfAnyRule() throws Exception {
        var value=new Row(registration(),1,T,Phase.EXPECTED,null,null);var encoded=MongoAlertNodeLifecycleStore.encode(value);
        try(var input=getClass().getResourceAsStream("/alert-node-lifecycle-v1.golden.json")) {
            assertThat(input).isNotNull();assertThat(encoded).isEqualTo(Document.parse(new String(input.readAllBytes(),java.nio.charset.StandardCharsets.UTF_8)));
        }
        assertThat(MongoAlertNodeLifecycleStore.decode(encoded)).isEqualTo(value);
        assertThat(MongoAlertConfigurationCodec.size(encoded)).isLessThanOrEqualTo(4096);
    }
    @Test void retiringIsAnIntentForTheOriginalBootWithItsOwnCommandAndTime() {
        var value=new Row(registration(),2,T,Phase.RETIRING,"67d93958-b5e1-4bb0-9273-f7bb29e48f01",T.plusSeconds(1));
        var row=MongoAlertNodeLifecycleStore.encode(value);assertThat(row).containsEntry("reason","ORDERLY_SHUTDOWN");
        assertThat(MongoAlertNodeLifecycleStore.decode(row)).isEqualTo(value);
        assertThat(row).doesNotContainKeys("recoveredAt","exitedAt","ruleIdentity","lastHeartbeat");
    }
    @Test void unknownAndCorruptAuthorityIsNotReinterpretedAsRetirement() {
        var original=MongoAlertNodeLifecycleStore.encode(new Row(registration(),1,T,Phase.EXPECTED,null,null));
        for(String field:List.of("formatVersion","revision","phase","memberId","setup","_id")) {
            var bad=Document.parse(original.toJson());bad.put(field,"unknown");
            assertThatThrownBy(()->MongoAlertNodeLifecycleStore.decode(bad)).isInstanceOfSatisfying(TapstateException.class,e->assertThat(e.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        }
        var bad=new Document(original).append("commandId","unexpected");
        assertThatThrownBy(()->MongoAlertNodeLifecycleStore.decode(bad)).isInstanceOf(TapstateException.class);
    }
    @Test void scopeAndBootAreSeparateLengthFramedIdentityComponents() {
        var a=new Key(new AlarmKey.Scope("a","bc"),"node","boot");var b=new Key(new AlarmKey.Scope("ab","c"),"node","boot");
        assertThat(MongoAlertNodeLifecycleStore.id(a)).isNotEqualTo(MongoAlertNodeLifecycleStore.id(b));
        assertThat(MongoAlertNodeLifecycleStore.id(registration().key())).hasSize(64);
        var original=registration();var large=new Registration(original.key(),new StorageSetupScope("cluster-a","x".repeat(6000),"setup-a",1),
                original.cache(),original.session(),original.memberId(),1);
        assertThatThrownBy(()->MongoAlertNodeLifecycleStore.encode(new Row(large,1,T,Phase.EXPECTED,null,null)))
                .isInstanceOfSatisfying(TapstateException.class,e->assertThat(e.code()).isEqualTo(AlertError.INVALID_CONFIGURATION));
    }
}
