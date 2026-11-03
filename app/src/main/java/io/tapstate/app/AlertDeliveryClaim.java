package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AlertStore;
import java.time.*;
import java.util.*;

/** Durable business-intent ownership. A successful claim is not a send permit. */
final class AlertDeliveryClaim {
    private final AlertStore store;
    private final String worker;
    private final Clock clock;
    private final Runnable active;
    AlertDeliveryClaim(AlertStore store,String worker,Clock clock,Runnable active){this.store=Objects.requireNonNull(store);this.worker=Objects.requireNonNull(worker);this.clock=Objects.requireNonNull(clock);this.active=Objects.requireNonNull(active);}
    Optional<AlertStore.Stored<AlertIntent.Record>> acquire(AlertIntent.Business identity){
        for(int retry=0;retry<3;retry++){
            active.run();var version=store.planning().version();var before=store.intent(identity,null);if(before.isEmpty())return Optional.empty();
            var original=before.get().value();var now=clock.instant();if(original.state().retained().stoppedAt()!=null)return Optional.empty();
            if(original.state().owner().activeAt(now)&&!original.state().owner().ownerId().equals(worker))return Optional.empty();
            var after=AlertIntentControl.claim(original,worker,now,now.plusSeconds(30));
            var outcome=store.commit(new AlertStore.Commit(AlertStore.CommitKind.PLAN,null,List.of(new AlertStore.IntentChange(before.get(),after)),List.of(),version));
            if(outcome!=AlertStore.Outcome.CONFLICT)return Optional.of(new AlertStore.Stored<>(before.get().revision()+(outcome==AlertStore.Outcome.STAGED?1:0),after));
        }
        throw new TapstateException(AlertError.COMMAND_CONFLICT,Map.of(),null);
    }
}
