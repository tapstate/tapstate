package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import java.time.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.adapters.mongostore.AlertReceiverFixtures.*;

class MongoAlertCandidateSecretsTest {
    private static final Instant T = Instant.parse("2026-10-28T00:00:00Z");
    private static final AlertTestCommand.Key KEY = new AlertTestCommand.Key(new AlarmKey.Scope("org-a", "cluster-a"), "verified-human", new AlertTestCommand.ClientId(T,"1".repeat(32)));
    private static AlertTestCommand.Parameters form(String target) { return new AlertTestCommand.Parameters("email",new AlertTestCommand.Candidate("form-a","0".repeat(64)),null,target,"binding-a",null,"a".repeat(64)); }
    private static AlertTestCommand.Accepted command(AlertTestCommand.Parameters p) { return new AlertTestCommand.Accepted(KEY,p,T,T.plusSeconds(300)); }
    @Test void randomEncryptionKeepsStableFullCommandDigestAndReplaysTheOriginalSnapshot() {
        try(var input=material(PASSWORD)) {
            var a=MongoAlertCandidateSecrets.prepare(KEY,form("target-a"),email().transport(),null,0,input,cipher());
            var b=MongoAlertCandidateSecrets.prepare(KEY,form("target-a"),email().transport(),null,0,input,cipher());
            assertThat(a.parameters()).isEqualTo(b.parameters()); assertThat(a.encrypted()).isNotEqualTo(b.encrypted());
            assertThat(a.toString()).doesNotContain(PASSWORD,a.encrypted().ciphertext());
            var accepted=command(a.parameters());
            assertThat(AlertTestCommand.admit(AlertChannelPolicy.Deployment.ON_PREM,new AlertChannelPolicy.Capability("email",1,AlertChannelPolicy.Family.OP_EXTERNAL),KEY,b.parameters(),accepted,T.plusSeconds(600)).command()).isEqualTo(accepted);
            try(var restored=MongoAlertCandidateSecrets.resolve(accepted,a.target(),a.encrypted(),cipher())) { assertThat(restored.authentication()).isEqualTo(input.authentication()); }
        }
    }
    @Test void changedPublicTargetOrSecretCannotAuthenticateUnderTheOldCandidateDigest() {
        try(var input=material(PASSWORD);var changed=material("changed-test-password")) {
            var a=MongoAlertCandidateSecrets.prepare(KEY,form("target-a"),email().transport(),null,0,input,cipher());
            var b=MongoAlertCandidateSecrets.prepare(KEY,form("target-b"),email().transport(),null,0,input,cipher());
            var c=MongoAlertCandidateSecrets.prepare(KEY,form("target-a"),email().transport(),null,0,changed,cipher());
            assertThat(a.parameters().candidate().digest()).isNotEqualTo(b.parameters().candidate().digest()).isNotEqualTo(c.parameters().candidate().digest());
            var tampered=new AlertTestCommand.Parameters("email",a.parameters().candidate(),null,"target-b","binding-a",null,"a".repeat(64));
            assertThatThrownBy(()->MongoAlertCandidateSecrets.resolve(command(tampered),a.target(),a.encrypted(),cipher())).isInstanceOf(TapstateException.class);
        }
    }
    @Test void fullActorScopeClientAndCandidateBindingPreventsCiphertextSubstitution() {
        try(var input=material(PASSWORD)) {
            var a=MongoAlertCandidateSecrets.prepare(KEY,form("target-a"),email().transport(),null,0,input,cipher());
            var foreign=new AlertTestCommand.Key(KEY.scope(),"other-human",KEY.clientId());
            var cmd=new AlertTestCommand.Accepted(foreign,a.parameters(),T,T.plusSeconds(300));
            assertThatThrownBy(()->MongoAlertCandidateSecrets.resolve(cmd,a.target(),a.encrypted(),cipher())).isInstanceOf(TapstateException.class);
            assertThatThrownBy(()->cipher().decrypt("alert-test-candidate-v1","arbitrary",a.encrypted().ciphertext())).isInstanceOf(TapstateException.class);
        }
    }
    @Test void exactPrivateWebhookEndpointIsEncryptedAndVerifiedWithItsPublicTransport() {
        var base=new AlertTestCommand.Parameters("webhook",form("target-a").candidate(),null,"target-a","binding-a",null,"a".repeat(64));
        try(var input=webhookMaterial()) {
            var a=MongoAlertCandidateSecrets.prepare(KEY,base,webhook().transport(),null,0,input,cipher());
            assertThat(a.encrypted().ciphertext()).doesNotContain("private-endpoint-token","test-bearer-token");
            try(var restored=MongoAlertCandidateSecrets.resolve(command(a.parameters()),a.target(),a.encrypted(),cipher())) { assertThat(restored.webhookEndpoint()).isEqualTo(ENDPOINT); }
        }
    }
    @Test void wrongKeyAndMissingCandidateRefuseWithoutRevealingMaterial() {
        try(var input=material(PASSWORD)) {
            var a=MongoAlertCandidateSecrets.prepare(KEY,form("target-a"),email().transport(),null,0,input,cipher());
            assertThatThrownBy(()->MongoAlertCandidateSecrets.resolve(command(a.parameters()),a.target(),null,cipher())).isInstanceOf(TapstateException.class);
            assertThatThrownBy(()->MongoAlertCandidateSecrets.resolve(command(a.parameters()),a.target(),a.encrypted(),new SourceConfigCipher(new byte[32]))).isInstanceOf(TapstateException.class);
        }
    }
}
