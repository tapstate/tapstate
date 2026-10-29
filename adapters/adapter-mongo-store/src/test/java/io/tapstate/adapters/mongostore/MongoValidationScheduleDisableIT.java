package io.tapstate.adapters.mongostore;

import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Instant;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;

/** Restrictive schedule changes use the original task CONFIG domain, independently of connector availability. */
@RequiresDocker
class MongoValidationScheduleDisableIT {
    @Container static final MongoDBContainer CONFIG=MongoValidationTaskStoreIT.CONFIG;
    @Container static final MongoDBContainer CACHE=MongoValidationTaskStoreIT.CACHE;
    @Test void staleDependenciesDoNotBlockOffOrRevokeAnAlreadyOpenOriginalPermission(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var proposal=f.create("flow");var interval=new ValidationSchedule.Interval(1,ValidationSchedule.Unit.MINUTES,Instant.EPOCH,"Asia/Shanghai");
            var settings=new ValidationTaskStore.Settings(new ValidationTaskStore.Full(ValidationMethod.COUNT,1,interval,true,10),proposal.settings().incremental());
            var create=new ValidationTaskStore.Create(proposal.task(),proposal.comparison(),settings,proposal.commandId());assertThat(f.tasks.create(create)).isEqualTo(ValidationTaskStore.Write.APPLIED);
            var before=f.read(create.task());var accepted=f.reserve(before,"admitted-before-off");
            var source=(SourceResource)f.configuration.artifacts().get("input").orElseThrow();var changed=new HashMap<>(source.config());changed.put("unavailable",true);
            f.configuration.artifacts().replace("input",CanonicalHash.of(source),new SourceResource(source.id(),source.metadata(),source.connector(),changed,source.mode(),source.tables(),source.srs(),source.execution(),source.experimental()));
            var enabled=f.read(create.task());var offSettings=new ValidationTaskStore.Settings(new ValidationTaskStore.Full(ValidationMethod.COUNT,1,interval,false,10),settings.incremental());
            assertThat(f.tasks.update(new ValidationTaskStore.Update(create.task(),1,create.comparison(),offSettings,UUID.randomUUID()))).isEqualTo(ValidationTaskStore.Write.CONFLICT);
            var off=new ValidationTaskStore.DisableSchedule(create.task(),1,UUID.randomUUID());
            assertThat(f.tasks.disableSchedule(off)).isEqualTo(ValidationTaskStore.Write.APPLIED);assertThat(f.tasks.disableSchedule(off)).isEqualTo(ValidationTaskStore.Write.DUPLICATE);
            var after=f.read(create.task());assertThat(after.revision()).isEqualTo(2);assertThat(after.settings()).isEqualTo(offSettings);assertThat(after.comparison()).isEqualTo(before.comparison());
            assertThat(after.full()).isEqualTo(enabled.full());assertThat(after.incremental()).isEqualTo(before.incremental());
            assertThat(after.schedulingRevision()).isEqualTo(before.schedulingRevision());assertThat(after.schedulingCommandId()).isEqualTo(before.schedulingCommandId());
            var stale=new ValidationReportAdmission.Start(create.task(),1,2,UUID.randomUUID().toString(),accepted.start().run());
            assertThat(accepted.access().admissions().reserve(stale)).isNotIn(ValidationReportAdmission.Reservation.RESERVED,ValidationReportAdmission.Reservation.DUPLICATE);
            assertThat(accepted.access().admissions().open(new ValidationReportAdmission.Open(accepted.start(),new ValidationRunSpec(before.comparison(),ValidationMode.FULL,ValidationMethod.COUNT))))
                    .isEqualTo(ValidationReportAdmission.Birth.CREATED);
            assertThat(f.read(create.task()).full().permission().phase()).isEqualTo(ValidationTaskStore.Phase.BORN);
        }
    }
    @Test void offAloneFencesTheOldRevisionWhileDependenciesAndTheFullSlotRemainAvailable(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var original=f.create("flow");var schedule=new ValidationSchedule.Cron("* * * * *","UTC");
            var settings=new ValidationTaskStore.Settings(new ValidationTaskStore.Full(ValidationMethod.COUNT,1,schedule,true,10),original.settings().incremental());
            var target=new ValidationTaskStore.Create(original.task(),original.comparison(),settings,original.commandId());f.tasks.create(target);
            var twin=f.create("flow");f.tasks.create(twin);var ownerTask=f.create("flow");f.tasks.create(ownerTask);
            var admitted=f.reserve(f.read(ownerTask.task()),"owner-positive");var owner=admitted.start().run().owner();
            var spec=new ValidationRunSpec(target.comparison(),ValidationMode.FULL,ValidationMethod.COUNT);
            var positiveIdentity=new ValidationRunIdentity(owner,twin.task().taskId(),"before-off",ValidationMode.FULL,spec.fingerprint(),1,target.comparison().comparisonRulesVersion());
            var positive=new ValidationReportAdmission.Start(twin.task(),1,1,UUID.randomUUID().toString(),positiveIdentity);
            assertThat(admitted.access().admissions().reserve(positive)).isEqualTo(ValidationReportAdmission.Reservation.RESERVED);
            var oldIdentity=new ValidationRunIdentity(owner,target.task().taskId(),"old-revision",ValidationMode.FULL,spec.fingerprint(),1,target.comparison().comparisonRulesVersion());
            var old=new ValidationReportAdmission.Start(target.task(),1,1,UUID.randomUUID().toString(),oldIdentity);
            assertThat(f.tasks.disableSchedule(new ValidationTaskStore.DisableSchedule(target.task(),1,UUID.randomUUID()))).isEqualTo(ValidationTaskStore.Write.APPLIED);
            assertThat(f.read(target.task()).full().permission()).isNull();
            assertThat(admitted.access().admissions().reserve(old)).isEqualTo(ValidationReportAdmission.Reservation.STALE);
            assertThat(f.read(target.task()).full().permission()).isNull();assertThat(f.read(target.task()).full().nextStartSequence()).isEqualTo(1);
        }
    }
    @Test void oldRepliesAndForeignLifetimesCannotCloseANewerConfiguration(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var proposal=f.create("flow");var interval=new ValidationSchedule.Cron("*/5 * * * *","UTC");var settings=new ValidationTaskStore.Settings(new ValidationTaskStore.Full(ValidationMethod.COUNT,1,interval,true,10),proposal.settings().incremental());
            var create=new ValidationTaskStore.Create(proposal.task(),proposal.comparison(),settings,proposal.commandId());f.tasks.create(create);var off=new ValidationTaskStore.DisableSchedule(create.task(),1,UUID.randomUUID());
            assertThat(f.tasks.disableSchedule(off)).isEqualTo(ValidationTaskStore.Write.APPLIED);
            assertThat(f.tasks.update(new ValidationTaskStore.Update(create.task(),2,create.comparison(),settings,UUID.randomUUID()))).isEqualTo(ValidationTaskStore.Write.APPLIED);
            assertThat(f.tasks.disableSchedule(off)).isEqualTo(ValidationTaskStore.Write.CONFLICT);assertThat(f.read(create.task()).settings().full().scheduleEnabled()).isTrue();
            var foreign=new ValidationReportAdmission.Task("cluster","flow",create.task().taskId(),UUID.randomUUID());
            assertThat(f.tasks.disableSchedule(new ValidationTaskStore.DisableSchedule(foreign,3,UUID.randomUUID()))).isEqualTo(ValidationTaskStore.Write.CONFLICT);
            var row=f.document(create.task());f.collection().updateOne(new Document("_id",row.get("_id")),new Document("$set",new Document("definition.settings.full.method","invalid")));
            assertThatThrownBy(()->f.tasks.disableSchedule(new ValidationTaskStore.DisableSchedule(create.task(),3,UUID.randomUUID()))).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            assertThat(f.collection().find(new Document("_id",row.get("_id"))).first().getLong("revision")).isEqualTo(3);
        }
    }
}
