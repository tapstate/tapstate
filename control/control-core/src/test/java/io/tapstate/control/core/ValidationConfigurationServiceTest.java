package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.probe.*;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationConfigurationServiceTest {
    private static final Clock CLOCK=Clock.fixed(Instant.parse("2026-10-29T00:00:00Z"),ZoneOffset.UTC);
    private static final UUID GENERATION=UUID.randomUUID();
    private static ValidationConfigurationService.Input input(String revision){return new ValidationConfigurationService.Input("flow","task",GENERATION.toString(),revision,"COUNT",
            List.of(new ValidationConfigurationService.Choice("orders","out","source","orders")),Map.of());}
    private static ValidationTaskSpec comparison(String hash){return new ValidationTaskSpec("flow",Map.of("flow",hash,"source","b".repeat(64),"target","c".repeat(64)),1,"direct-full-v1",
            List.of(new ValidationTaskSpec.Mapping("orders",ValidationTaskSpec.OutputKind.SYNC,"out",new ValidationTaskSpec.Table("source","source",null,null,"orders",null,null),
                    new ValidationTaskSpec.Table("out","target",null,null,"checked_orders",null,null),null)));}
    private static class Store implements ValidationTaskStore {
        Snapshot current;int creates,updates;
        @Override public Optional<Snapshot> read(String pipeline,String task){return Optional.ofNullable(current);}
        @Override public Write create(Create command){creates++;return Write.APPLIED;}
        @Override public Write update(Update command){updates++;return Write.APPLIED;}
        @Override public ValidationReportAdmission.Closure close(ValidationReportAdmission.Task task){throw new AssertionError("close not called");}
    }
    private static final class Probe implements ValidationEvaluationProbe {
        int calls,closes;boolean active;String fingerprint="a".repeat(64);ValidationTaskSpec spec=comparison("a".repeat(64));
        @Override public ValidationEvaluation evaluate(ValidationEvaluation.Request request){
            calls++;active=true;
            assertThat(request.previous()).satisfiesAnyOf(value->assertThat(value).isNull(),value->assertThat(value.pipelineId()).isEqualTo("flow"));
            return new ValidationEvaluation(){
                @Override public View view(){requireCurrent();return new View("COUNT","9007199254740993","1",fingerprint,List.of(),"INITIAL_SYNC_FINISHED_AND_BOTH_SIDES_STABLE","READ_OBSERVATIONS_NO_COMMON_TIME_SNAPSHOT",null);}
                @Override public Optional<ValidationTaskSpec> comparison(){requireCurrent();return Optional.of(spec);}
                @Override public void requireCurrent(){assertThat(active).isTrue();}
                @Override public void close(){active=false;closes++;}
            };
        }
    }
    private static ValidationConfigurationService service(Store store,Probe probe,AuditStore audit){
        return new ValidationConfigurationService("cluster",store,probe,new ValidationSchedule.Calculator(){
            @Override public ValidationSchedule.Preview preview(ValidationSchedule schedule,Instant base,long revision,ValidationSchedule.SearchBudget budget){throw new AssertionError("no schedule");}
            @Override public Instant nextAfter(ValidationSchedule schedule,Instant after,ValidationSchedule.SearchBudget budget){throw new AssertionError("no scheduler");}
        },new AuditGate(audit,CLOCK),CLOCK);
    }
    @Test void projectionKeepsLeaseUntilConsumerFinishesAndPreservesExactWireRevision(){
        var probe=new Probe();var store=new Store();var service=service(store,probe,ignored->{});
        var evaluated=service.evaluate(input("0"));assertThat(probe.active).isTrue();assertThat(evaluated.view().configurationRevision()).isEqualTo("9007199254740993");
        assertThat(store.creates+store.updates).isZero();evaluated.close();assertThat(probe.active).isFalse();assertThat(probe.closes).isOne();
    }
    @Test void saveReevaluatesRatherThanAcceptingClientFingerprintAsAuthority(){
        var probe=new Probe();var store=new Store();var audits=new ArrayList<AuditRecord>();var service=service(store,probe,audits::add);
        var command=new ValidationConfigurationService.Save(input("0"),"a".repeat(64),UUID.randomUUID().toString(),10,null,false);
        try(var first=service.evaluate(command.input())){assertThat(first.view().fingerprint()).isEqualTo(command.fingerprint());}
        probe.fingerprint="d".repeat(64);probe.spec=comparison("d".repeat(64));
        assertThat(service.save("alice",command).outcome()).isEqualTo("CONFLICT");assertThat(store.creates).isZero();assertThat(probe.calls).isEqualTo(2);assertThat(probe.active).isFalse();
        assertThat(audits).singleElement().satisfies(record->{assertThat(record.operationId()).isEqualTo("validation.save");assertThat(record.principal()).isEqualTo("alice");});
    }
    @Test void failedAuditNeverInvokesMetadataOrConfigMutation(){
        var probe=new Probe();var store=new Store();var service=service(store,probe,ignored->{throw new IllegalStateException("audit unavailable");});
        assertThatThrownBy(()->service.save("alice",new ValidationConfigurationService.Save(input("0"),"a".repeat(64),UUID.randomUUID().toString(),10,null,false)))
                .isInstanceOf(TapstateException.class).hasMessageContaining("audit-blocked");
        assertThat(probe.calls).isZero();assertThat(store.creates+store.updates).isZero();
    }
    @Test void saveUsesOneAdmittedEvaluationAndOriginalStoreWrite(){
        var store=new Store(){@Override public Write create(Create command){assertThat(command.settings().full().maxConcurrentTables()).isOne();return super.create(command);}};
        var probe=new Probe();var service=service(store,probe,ignored->{});
        assertThat(service.save("alice",new ValidationConfigurationService.Save(input("0"),probe.fingerprint,UUID.randomUUID().toString(),10,null,false)).outcome()).isEqualTo("APPLIED");
        assertThat(store.creates).isOne();assertThat(probe.calls).isOne();assertThat(probe.active).isFalse();
    }
    @Test void malformedScheduleInputsAreCodedRefusals(){
        var probe=new Probe();var store=new Store();var service=service(store,probe,ignored->{});
        assertThatThrownBy(()->service.preview(null,"1")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(()->service.preview(new ValidationConfigurationService.Schedule("INTERVAL","1","MINUTES","bad-date",null,"Asia/Shanghai"),"1"))
                .isInstanceOf(TapstateException.class);
    }
    @Test void malformedAndNumericWireInputsDoNotReachMetadata(){
        var probe=new Probe();var store=new Store();var service=service(store,probe,ignored->{});
        for(var revision:List.of("01","-1","1.0","9223372036854775808"))assertThatThrownBy(()->service.evaluate(input(revision))).isInstanceOf(TapstateException.class);
        assertThat(probe.calls).isZero();
    }
}
