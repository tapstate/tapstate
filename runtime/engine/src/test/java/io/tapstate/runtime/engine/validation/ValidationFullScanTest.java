package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.ValidationRowCursor;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.core.validation.ValidationRootCheck.*;

class ValidationFullScanTest {
    static final ValidationValues.Limits ROW=new ValidationValues.Limits(4096,64,8);
    static final Limits RESULT=new Limits(65536,16,16,8,0);
    @Test void bothTailsAndDuplicateGroupsBecomeFreshRootChecksWithoutAPreviewLimit() {
        var f=new Fixture();var checked=new AtomicInteger();var published=new AtomicInteger();
        var mapping=new ValidationFullScan.Mapping("mapping",guard->merge(new Rows(25001),new Rows(25002)),(observation,guard)->{guard.run();checked.incrementAndGet();return result(observation);});
        var scan=new ValidationFullScan(f.identity,f.spec,List.of(mapping),()->{},check->{published.incrementAndGet();});
        scan.run();assertThat(checked).hasValue(25002);assertThat(published).hasValue(25002);assertThat(scan.complete()).isTrue();
        assertThat(scan.completedMappings()).isOne();assertThat(scan.readingMapping()).isNull();
        assertThatThrownBy(scan::run).isInstanceOf(TapstateException.class).hasMessageContaining("alreadyAttempted");
    }
    @Test void aLostPublicationReplyCannotCompleteOrSkipToTheNextRoot() {
        var f=new Fixture();var checked=new AtomicInteger();var writes=new AtomicInteger();
        var scan=new ValidationFullScan(f.identity,f.spec,List.of(new ValidationFullScan.Mapping("mapping",guard->merge(new Rows(3),new Rows(3)),
                (observation,guard)->{checked.incrementAndGet();return result(observation);})),()->{},check->{writes.incrementAndGet();throw unknown();});
        assertThatThrownBy(scan::run).isInstanceOf(TapstateException.class).hasMessageContaining("unknown");
        assertThat(checked).hasValue(1);assertThat(writes).hasValue(1);assertThat(scan.complete()).isFalse();
        assertThat(scan.completedMappings()).isZero();assertThat(scan.readingMapping()).isEqualTo("mapping");
    }
    @Test void aForeignRootResultNeverPublishesAndBothReadersClose() {
        var f=new Fixture();var source=new Rows(2);var target=new Rows(2);var writes=new AtomicInteger();
        var scan=new ValidationFullScan(f.identity,f.spec,List.of(new ValidationFullScan.Mapping("mapping",guard->merge(source,target),
                (observation,guard)->result(new ValidationObservation(observation.run(),new ValidationRoot("other","out",observation.root().rootKey(),List.of()),1)))),()->{},check->writes.incrementAndGet());
        assertThatThrownBy(scan::run).isInstanceOf(TapstateException.class).hasMessageContaining("rootResult");
        assertThat(writes).hasValue(0);assertThat(source.closed).isTrue();assertThat(target.closed).isTrue();assertThat(scan.complete()).isFalse();
    }
    @Test void cancellationBetweenCheckingAndPublicationCannotPublishLateResults() {
        var f=new Fixture();var original=new AtomicReference<ValidationFullScan>();var writes=new AtomicInteger();
        var scan=new ValidationFullScan(f.identity,f.spec,List.of(new ValidationFullScan.Mapping("mapping",guard->merge(new Rows(2),new Rows(2)),
                (observation,guard)->{original.get().cancel();return result(observation);})),()->{},check->writes.incrementAndGet());original.set(scan);
        assertThatThrownBy(scan::run).isInstanceOfSatisfying(TapstateException.class,error->assertThat(error.code()).isEqualTo(ValidationError.EXECUTION_CANCELLED));
        assertThat(writes).hasValue(0);assertThat(scan.complete()).isFalse();
    }
    @Test void aPhysicalTableOutsideTheFixedMappingIsRejectedBeforeBusinessChecking() {
        var f=new Fixture();var checked=new AtomicInteger();
        var source=groups(new Rows(1),"foreign");var target=groups(new Rows(1),"target");
        var scan=new ValidationFullScan(f.identity,f.spec,List.of(new ValidationFullScan.Mapping("mapping",guard->new ValidationOrderedMerge(source,target),
                (observation,guard)->{checked.incrementAndGet();return result(observation);})),()->{},check->{});
        assertThatThrownBy(scan::run).isInstanceOf(TapstateException.class).hasMessageContaining("physicalTable");assertThat(checked).hasValue(0);
    }
    static ValidationRootCheck result(ValidationObservation observation) {
        var b=builder(observation,ValidationMethod.FULL_COLUMN,ValidationCheckEvent.Participation.SHOULD_EXIST,TargetPresence.PRESENT,RESULT);
        var boundary=new ValidationCheckEvent.ReadBoundary("source","test-read-v1",Instant.EPOCH,Instant.EPOCH);
        b.read(Side.SOURCE,boundary).read(Side.TARGET,new ValidationCheckEvent.ReadBoundary("target","test-read-v1",Instant.EPOCH,Instant.EPOCH));
        for(var check:Check.values())b.checked(check);return b.finish();
    }
    static ValidationOrderedMerge merge(Rows left,Rows right){return new ValidationOrderedMerge(groups(left,"source"),groups(right,"target"));}
    static ValidationOrderedGroups groups(Rows rows,String side){return new ValidationOrderedGroups(side,rows,row->{
        var key=new ValidationKey(List.of(ValidationKey.Part.number(((Number)row.fields().get("id")).longValue())));
        return new ValidationOrderedGroups.Key(key,new ValidationPhysicalIdentity(side,"db",null,"orders",List.of(List.of("id")),key));
    },ROW.bytes());}
    static final class Rows implements ValidationRowCursor {
        final int count;int index;boolean closed;Rows(int count){this.count=count;}
        public Optional<Row> next(){if(index==count)return Optional.empty();int ordinal=index++;return Optional.of(new Row(){
            public long ordinal(){return ordinal;}public ValidationValues.Row values(){return ValidationValues.ownRow(Map.of("id",ordinal),ROW);}public void close(){}
        });}
        public void close(){closed=true;}
    }
    static final class Fixture {
        final ValidationRunSpec spec;final ValidationRunIdentity identity;
        Fixture(){var keys=List.of(new ValidationKeyBinding.Component(List.of("id"),ValidationKey.Kind.EXACT_NUMBER,Set.of("exact-number")));
            var task=new ValidationTaskSpec("flow",Map.of("flow","a".repeat(64),"source","b".repeat(64),"target","c".repeat(64)),1,"rules",List.of(new ValidationTaskSpec.Mapping("mapping",ValidationTaskSpec.OutputKind.SYNC,"out",
                    new ValidationTaskSpec.Table("input","source","db",null,"orders","model","codec"),new ValidationTaskSpec.Table("out","target","db",null,"orders","model","codec"),new ValidationTaskSpec.Keys("confirmed",keys,keys))));
            spec=new ValidationRunSpec(task,ValidationMode.FULL,ValidationMethod.FULL_COLUMN);identity=new ValidationRunIdentity(new ValidationOwner("cluster","flow","node","boot",1,1,1),"task","run",ValidationMode.FULL,spec.fingerprint(),1,"rules");}
    }
    private static TapstateException unknown(){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","test.unknown"),null);}
}
