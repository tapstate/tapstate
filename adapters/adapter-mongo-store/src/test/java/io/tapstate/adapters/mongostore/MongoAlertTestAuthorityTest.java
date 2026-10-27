package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MongoAlertTestAuthorityTest {
    @Test void exactOriginalAuthorityUsesIntentVersionTwoWhileLegacyTestFactsStayReadable() throws Exception {
        var actor=new AlertTestAuthority(AlertTestAuthority.Mechanism.MACHINE_TOKEN,"installation-a","actor-a","ADMIN","a".repeat(64),null);
        try(var input=getClass().getResourceAsStream("/alert-test-authority-v1.golden.json")){
            assertThat(MongoAlertDeliveryCodec.authority(actor)).isEqualTo(Document.parse(new String(input.readAllBytes(),java.nio.charset.StandardCharsets.UTF_8)));
        }
        var old=AlertDeliveryFixtures.test();var f=old.frozen();var command=((AlertIntent.Test)f.identity()).command();
        var key=new AlertTestCommand.Key(command.key().scope(),actor.principalKey(),command.key().clientId());
        var accepted=new AlertTestCommand.Accepted(key,command.parameters(),command.acceptedAt(),command.deadline());
        var frozen=new AlertIntent.Frozen(new AlertIntent.Test(accepted),f.channel(),f.route(),f.target(),f.content(),f.members(),f.retry(),f.retention(),f.createdAt(),f.deadline(),actor);
        var record=AlertDeliveryFixtures.initial(frozen,old.state().payload(),old.state().candidate());var row=MongoAlertDeliveryCodec.write(record,1);
        assertThat(row.getInteger("formatVersion")).isEqualTo(2);assertThat(MongoAlertDeliveryCodec.readTest(key,row).value()).isEqualTo(record);
        assertThat(MongoAlertDeliveryCodec.readTest(command.key(),MongoAlertDeliveryCodec.write(old,1)).value()).isEqualTo(old);
        row.put("formatVersion",1);assertThatThrownBy(()->MongoAlertDeliveryCodec.readTest(key,row)).isInstanceOf(TapstateException.class);
    }
    @Test void realMechanismPreventsHumanMachineCollisionAndCredentialRotationDoesNotResetActorQuota() {
        var machine=new AlertTestAuthority(AlertTestAuthority.Mechanism.MACHINE_TOKEN,"installation-a","same-subject","ADMIN","a".repeat(64),null);
        var rotated=new AlertTestAuthority(machine.mechanism(),machine.realm(),machine.subject(),machine.grantedScope(),"b".repeat(64),null);
        var human=new AlertTestAuthority(AlertTestAuthority.Mechanism.HUMAN_JWT,machine.realm(),machine.subject(),"ADMIN","a".repeat(64),AlertDeliveryFixtures.AT.plusSeconds(900));
        assertThat(rotated.principalKey()).isEqualTo(machine.principalKey());assertThat(human.principalKey()).isNotEqualTo(machine.principalKey());
    }
}
