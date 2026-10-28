package io.tapstate.app;

import io.tapstate.control.core.*;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class CloudAlertAccessTest {
    @Test void callbacksUseOnlyOriginalLocalEvidenceWhileIoStaysOutside(){
        var storage=new CacheAuthScope("cluster",1,StorageBinding.Mode.CLOUD,"cache",1);
        var io=new AtomicInteger();var local=new AtomicInteger();var active=new AtomicBoolean(true);
        var caller=new CredentialAuthentication(new VerifiedToken("user",Scope.ADMIN),Optional.of(storage),Optional.of("org"),io::incrementAndGet,
                Optional.of(new CredentialEvidence(CredentialType.CLOUD_SESSION,Optional.empty(),()->{if(!active.get())throw AlertHttpModel.stale("retired-session");local.incrementAndGet();})));
        var access=CloudAlertAccess.bind(caller,new AlarmKey.Scope("org","cluster"),storage,()->{});assertThat(io).hasValue(1);
        access.locallyActive().run();access.locallyActive().run();assertThat(io).hasValue(1);assertThat(local).hasValue(3);
        access.revalidate().run();assertThat(io).hasValue(2);active.set(false);assertThatThrownBy(()->access.locallyActive().run()).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        assertThatThrownBy(()->CloudAlertAccess.bind(caller,new AlarmKey.Scope("other","cluster"),storage,()->{})).isInstanceOf(io.tapstate.core.common.TapstateException.class);
    }
}
