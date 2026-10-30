package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.ValidationRowCursor;
import java.util.*;
import java.time.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static io.tapstate.runtime.engine.validation.ValidationFullScanTest.*;

class ValidationDirectOrderedScanTest {
    @Test void allRootsUseOneStreamPerSideAndPublishBeforeTheBorrowedPairCloses(){
        var f=new Fixture();var source=new Rows(25001);var target=new Rows(25002);
        var opens=new AtomicInteger();var content=new AtomicInteger();var published=new AtomicInteger();var extras=new AtomicInteger();
        var borrowed=new AtomicReference<ValidationOrderedMerge.Pair>();
        var mapping=ValidationFullScan.Mapping.paired("mapping",guard->{opens.addAndGet(2);return merge(source,target);},
                (observation,pair,guard)->{
                    borrowed.set(pair);
                    return ValidationRootReader.readOrderedDirect(observation,ValidationMethod.FULL_COLUMN,pair,RESULT,guard,(rows,result)->{
                        content.incrementAndGet();assertThat(rows.source().original().fields()).isEqualTo(rows.target().original().fields());
                        assertThat(rows.sourceRead().contractVersion()).isEqualTo("independent-ordered-prefix-v1");
                        assertThat(rows.sourceSample().orElseThrow().readId()).isEqualTo(rows.sourceRead().readId());
                    });
                });
        var scan=new ValidationFullScan(f.identity,f.spec,List.of(mapping),()->{},result->{
            borrowed.get().state();published.incrementAndGet();
            if(result.counts().containsKey(Kind.TARGET_EXTRA))extras.incrementAndGet();
        });
        scan.run();assertThat(scan.complete()).isTrue();assertThat(opens).hasValue(2);
        assertThat(content).hasValue(25001);assertThat(published).hasValue(25002);assertThat(extras).hasValue(1);
        assertThat(source.nextCalls).isEqualTo(25002);assertThat(target.nextCalls).isEqualTo(25003);
        assertThat(source.rowsClosed).isEqualTo(25001);assertThat(target.rowsClosed).isEqualTo(25002);
        assertThat(source.closed&&target.closed).isTrue();
        assertThatThrownBy(()->borrowed.get().state()).isInstanceOf(IllegalStateException.class);
    }

    @Test void completeDuplicatesKeepMultiplicityAndMissingTargetWithoutAContentWinner(){
        for(var method:List.of(ValidationMethod.UNIQUE_KEY,ValidationMethod.FULL_COLUMN))for(int targetCount:List.of(0,1,30000)){
            var f=new Fixture(method);var source=new RepeatedRows(30000);var target=new RepeatedRows(targetCount);
            var results=new ArrayList<ValidationRootCheck>();
            var mapping=ValidationFullScan.Mapping.paired("mapping",guard->new ValidationOrderedMerge(groups(source,"source"),groups(target,"target")),
                    (observation,pair,guard)->ValidationRootReader.readOrderedDirect(observation,method,pair,RESULT,guard,
                            (rows,result)->{throw new AssertionError("duplicated source must never choose content");}));
            var scan=new ValidationFullScan(f.identity,f.spec,List.of(mapping),()->{},results::add);scan.run();
            assertThat(scan.complete()).isTrue();assertThat(source.delivered).isEqualTo(30000);assertThat(target.delivered).isEqualTo(targetCount);
            assertThat(source.rowsClosed).isEqualTo(30000);assertThat(target.rowsClosed).isEqualTo(targetCount);
            assertThat(results).singleElement().satisfies(result->{
                assertThat(result.participation()).isEqualTo(ValidationCheckEvent.Participation.SHOULD_EXIST);
                assertThat(result.targetPresence()).isEqualTo(targetCount==0?TargetPresence.ABSENT:TargetPresence.PRESENT);
                assertThat(result.counts()).containsEntry(Kind.SOURCE_DUPLICATE,1L);
                assertThat(result.counts().containsKey(Kind.TARGET_MISSING)).isEqualTo(targetCount==0);
                assertThat(result.counts().containsKey(Kind.TARGET_DUPLICATE)).isEqualTo(targetCount>1);
                assertThat(result.counts()).doesNotContainKeys(Kind.TARGET_VALUE,Kind.TARGET_TYPE);
                assertThat(result.checks()).containsEntry(Check.STRUCTURE,Status.BLOCKED);
                assertThat(result.findings().keySet()).anySatisfy(finding->{
                    assertThat(finding.kind()).isEqualTo(Kind.SOURCE_DUPLICATE);
                    assertThat(finding.multiplicity()).contains(new Multiplicity(30000,true));
                });
            });
        }
    }

    @Test void cancellationBeforePublicationClosesThePairAndCannotPublishLate(){
        var f=new Fixture();var source=new Rows(3);var target=new Rows(3);var original=new AtomicReference<ValidationFullScan>();
        var writes=new AtomicInteger();var borrowed=new AtomicReference<ValidationOrderedMerge.Pair>();
        var mapping=ValidationFullScan.Mapping.paired("mapping",guard->merge(source,target),(observation,pair,guard)->{
            borrowed.set(pair);var result=ValidationRootReader.readOrderedDirect(observation,ValidationMethod.FULL_COLUMN,pair,RESULT,guard,(rows,builder)->{});
            original.get().cancel();return result;
        });
        var scan=new ValidationFullScan(f.identity,f.spec,List.of(mapping),()->{},result->writes.incrementAndGet());original.set(scan);
        assertThatThrownBy(scan::run).isInstanceOfSatisfying(TapstateException.class,
                error->assertThat(error.code()).isEqualTo(ValidationError.EXECUTION_CANCELLED));
        assertThat(scan.complete()).isFalse();assertThat(writes).hasValue(0);assertThat(source.closed&&target.closed).isTrue();
        assertThatThrownBy(()->borrowed.get().state()).isInstanceOf(IllegalStateException.class);
    }

    @Test void aLateTailFailureOrUnknownPublicationNeverCompletesTheMapping(){
        for(boolean tailFailure:List.of(true,false)){
            var f=new Fixture();var source=new Rows(4);var target=new Rows(4);var writes=new AtomicInteger();
            var failed=new TapstateException(ValidationError.READ_FAILED,Map.of("table","target"),null);
            ValidationRowCursor targetRead=new ValidationRowCursor(){
                public Optional<Row> next(){if(tailFailure&&target.index==4)throw failed;return target.next();}
                public void close(){target.close();}
            };
            var mapping=ValidationFullScan.Mapping.paired("mapping",guard->new ValidationOrderedMerge(groups(source,"source"),groups(targetRead,"target")),
                    (observation,pair,guard)->ValidationRootReader.readOrderedDirect(observation,ValidationMethod.FULL_COLUMN,pair,RESULT,guard,(rows,result)->{}));
            var scan=new ValidationFullScan(f.identity,f.spec,List.of(mapping),()->{},result->{writes.incrementAndGet();if(!tailFailure)throw failed;});
            assertThat(catchThrowable(scan::run)).isSameAs(failed);assertThat(scan.complete()).isFalse();assertThat(scan.completedMappings()).isZero();
            assertThat(source.closed&&target.closed).isTrue();if(!tailFailure)assertThat(writes).hasValue(1);
        }
    }

    @Test void pendingAndEofSidesReuseTheirActualConsumedPrefixTimesAndReadIds(){
        var clock=new StepClock();var source=new TimedRows(clock,1,3);var target=new TimedRows(clock,0,2,3,4);
        try(var merge=new ValidationOrderedMerge(timed(source,"source-read",clock),timed(target,"target-read",clock))){
            ValidationCheckEvent.ReadBoundary firstSource;
            try(var pair=merge.next().orElseThrow()){
                assertThat(pair.source()).isEmpty();firstSource=pair.sourceRead();
                assertThat(firstSource.startedAt()).isEqualTo(Instant.EPOCH);
                assertThat(firstSource.finishedAt()).isEqualTo(Instant.EPOCH.plusMillis(20));
                assertThat(pair.targetRead().startedAt()).isEqualTo(Instant.EPOCH.plusMillis(20));
                assertThat(pair.targetRead().readId()).isEqualTo("target-read");
            }
            clock.now=clock.now.plusSeconds(5);
            try(var pair=merge.next().orElseThrow()){
                assertThat(pair.target()).isEmpty();assertThat(pair.sourceRead()).isEqualTo(firstSource);
                assertThat(pair.targetRead().readId()).isEqualTo("target-read");
                assertThat(pair.targetRead().finishedAt()).isAfter(firstSource.finishedAt());
            }
            try(var pair=merge.next().orElseThrow()){assertThat(pair.source()).isEmpty();}
            ValidationCheckEvent.ReadBoundary atSourceEof;
            try(var pair=merge.next().orElseThrow()){
                assertThat(pair.state()).isEqualTo(ValidationCheckEvent.PairState.MATCHED);atSourceEof=pair.sourceRead();
                assertThat(atSourceEof.readId()).isEqualTo("source-read");
            }
            clock.now=clock.now.plusSeconds(30);
            try(var pair=merge.next().orElseThrow()){
                assertThat(pair.source()).isEmpty();assertThat(pair.sourceRead()).isEqualTo(atSourceEof);
                assertThat(pair.targetRead().finishedAt()).isAfter(atSourceEof.finishedAt());
            }
            assertThat(merge.next()).isEmpty();assertThat(source.calls).isEqualTo(3);assertThat(target.calls).isEqualTo(5);
        }
    }

    @Test void anEofCloseFailureCannotTurnPublishedObservationsIntoACompleteRun(){
        var f=new Fixture();var source=new Rows(3);var target=new Rows(3);var writes=new AtomicInteger();
        var failure=new TapstateException(ValidationError.READ_FAILED,Map.of("table","close"),null);
        ValidationRowCursor failing=new ValidationRowCursor(){
            public Optional<Row> next(){return source.next();}
            public void close(){source.close();throw failure;}
        };
        var mapping=ValidationFullScan.Mapping.paired("mapping",guard->new ValidationOrderedMerge(groups(failing,"source"),groups(target,"target")),
                (observation,pair,guard)->ValidationRootReader.readOrderedDirect(observation,ValidationMethod.FULL_COLUMN,pair,RESULT,guard,(rows,result)->{}));
        var scan=new ValidationFullScan(f.identity,f.spec,List.of(mapping),()->{},result->writes.incrementAndGet());
        assertThat(catchThrowable(scan::run)).isSameAs(failure);assertThat(writes).hasValue(3);
        assertThat(scan.complete()).isFalse();assertThat(scan.completedMappings()).isZero();assertThat(target.closed).isTrue();
    }

    static ValidationOrderedGroups timed(ValidationRowCursor rows,String side,Clock clock){
        return new ValidationOrderedGroups(side,rows,row->{
            var key=new ValidationKey(List.of(ValidationKey.Part.number(((Number)row.fields().get("id")).longValue())));
            return new ValidationOrderedGroups.Key(key,new ValidationPhysicalIdentity(side,"db",null,"orders",List.of(List.of("id")),key));
        },ROW.bytes(),clock);
    }
    static final class StepClock extends Clock {
        Instant now=Instant.EPOCH;
        public ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(ZoneId zone){return this;}
        public Instant instant(){return now;}
    }
    static final class TimedRows implements ValidationRowCursor {
        final StepClock clock;final int[] values;int index,calls;
        TimedRows(StepClock clock,int...values){this.clock=clock;this.values=values;}
        public Optional<Row> next(){calls++;clock.now=clock.now.plusMillis(10);if(index==values.length)return Optional.empty();
            int ordinal=index++;int value=values[ordinal];return Optional.of(new Row(){
                public long ordinal(){return ordinal;}public ValidationValues.Row values(){return ValidationValues.ownRow(Map.of("id",value),ROW);}
                public void close(){}
            });
        }
        public void close(){}
    }

    static final class RepeatedRows implements ValidationRowCursor {
        final int count;int delivered,rowsClosed;RepeatedRows(int count){this.count=count;}
        public Optional<Row> next(){if(delivered==count)return Optional.empty();int ordinal=delivered++;return Optional.of(new Row(){
            public long ordinal(){return ordinal;}public ValidationValues.Row values(){return ValidationValues.ownRow(Map.of("id",7),ROW);}
            public void close(){rowsClosed++;}
        });}
        public void close(){}
    }
}
