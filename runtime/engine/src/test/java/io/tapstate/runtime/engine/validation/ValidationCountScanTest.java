package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationCountScanTest {
    private static final ValidationTaskSpec.Table SOURCE=new ValidationTaskSpec.Table("input","input","db",null,"left",null,null);
    private static final ValidationTaskSpec.Table TARGET=new ValidationTaskSpec.Table("out","target","db",null,"right",null,null);
    private static final ValidationRunSpec SPEC=new ValidationRunSpec(new ValidationTaskSpec("flow",Map.of("flow","a".repeat(64),"input","b".repeat(64),"target","c".repeat(64)),
            1,"rules",List.of(new ValidationTaskSpec.Mapping("mapping",ValidationTaskSpec.OutputKind.SYNC,"out",SOURCE,TARGET,null))),ValidationMode.FULL,ValidationMethod.COUNT);
    private static final ValidationRunIdentity RUN=new ValidationRunIdentity(new ValidationOwner("cluster","flow","node","boot",1,1,0),"task","run",ValidationMode.FULL,SPEC.fingerprint(),1,"rules");
    private static ValidationCountScan.Scalar scalar(ValidationTaskSpec.Table table,long count) {
        return new ValidationCountScan.Scalar(table,count,new ValidationCheckEvent.ReadBoundary("native","native-count-v1",Instant.EPOCH,Instant.EPOCH));
    }
    @Test void completePublicationIsRequiredAndExactCountsDoNotBecomeRowOrContentClaims() {
        var observed=new ArrayList<ValidationCountCheck>();
        var scan=new ValidationCountScan(RUN,SPEC,List.of(new ValidationCountScan.Mapping("mapping",guard->scalar(SOURCE,9007199254740993L),guard->scalar(TARGET,9007199254740992L))),()->{},observed::add);
        assertThat(scan.completedMappings()).isZero();assertThat(scan.readingMapping()).isNull();
        scan.run();assertThat(scan.complete()).isTrue();assertThat(scan.completedMappings()).isOne();assertThat(scan.readingMapping()).isNull();assertThat(observed).singleElement().satisfies(value->{
            assertThat(value.sourceCount()).isEqualTo(9007199254740993L);assertThat(value.difference()).isOne();assertThat(value.sameCount()).isFalse();
        });
        var failed=new ValidationCountScan(RUN,SPEC,List.of(new ValidationCountScan.Mapping("mapping",guard->scalar(SOURCE,0),guard->scalar(TARGET,0))),()->{},value->{throw new IllegalStateException("unknown publication");});
        assertThatThrownBy(failed::run).hasMessage("unknown publication");assertThat(failed.complete()).isFalse();
        assertThat(failed.completedMappings()).isZero();assertThat(failed.readingMapping()).isEqualTo("mapping");
        assertThatThrownBy(scan::run).isInstanceOf(TapstateException.class);
    }
    @Test void aForeignPhysicalScalarCannotBePublishedOrAdvanceToTheTargetRead() {
        var target=new AtomicInteger();var published=new AtomicInteger();
        var scan=new ValidationCountScan(RUN,SPEC,List.of(new ValidationCountScan.Mapping("mapping",guard->scalar(TARGET,0),guard->{target.incrementAndGet();return scalar(TARGET,0);})),()->{},value->published.incrementAndGet());
        assertThatThrownBy(scan::run).isInstanceOf(TapstateException.class).hasMessageContaining("sourceTable");
        assertThat(target).hasValue(0);assertThat(published).hasValue(0);assertThat(scan.complete()).isFalse();
    }
    @Test void cancellationBetweenTheTwoCountsDoesNotPublishAnEmptyOrPartialTable() {
        var holder=new ValidationCountScan[1];var published=new AtomicInteger();var target=new AtomicInteger();
        holder[0]=new ValidationCountScan(RUN,SPEC,List.of(new ValidationCountScan.Mapping("mapping",guard->{holder[0].cancel();return scalar(SOURCE,0);},
                guard->{target.incrementAndGet();return scalar(TARGET,0);})),()->{},value->published.incrementAndGet());
        assertThatThrownBy(holder[0]::run).isInstanceOf(TapstateException.class);assertThat(target).hasValue(0);assertThat(published).hasValue(0);
        assertThat(holder[0].complete()).isFalse();
    }
}
