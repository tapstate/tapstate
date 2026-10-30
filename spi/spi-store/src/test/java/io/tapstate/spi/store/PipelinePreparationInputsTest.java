package io.tapstate.spi.store;

import io.tapstate.core.model.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PipelinePreparationInputsTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",1,"incarnation","flow",1);
    private static SourceResource source(String database,Metadata metadata) {
        return new SourceResource("input",metadata,"mongo",Map.of("database",database),SourceMode.CDC,List.of(TableRef.literal("orders")),new Srs("fixed",null,null,null,false),null);
    }
    private static PipelineResource pipeline(boolean shared,ReadMode mode,String start,Metadata metadata) {
        return new PipelineResource("flow",metadata,List.of(SourceRef.spec("input",shared)),null,null,null,new Settings(null,null,null,null,mode,start),null);
    }
    private static PipelinePreparationInputs inputs(PipelineResource pipeline,SourceResource source) { return PipelinePreparationInputs.of(SCOPE,pipeline,Map.of("input",source),Map.of("input",Set.of("orders"))); }
    @Test void anExplicitSharedKeyCannotHideAChangedPhysicalSourceBehindTheSamePrivateId() {
        var pipeline = pipeline(false,ReadMode.CDC_ONLY,"latest",null); var old = inputs(pipeline,source("x",null)); var next = inputs(pipeline,source("y",null));
        assertThat(next.directChannels().getFirst().context()).isEqualTo(old.directChannels().getFirst().context());
        assertThat(next.sameRecovery(old)).isFalse();
    }
    @Test void metadataChangesAreFreshDefinitionsOfTheSameRecoveryIdentity() {
        var old = inputs(pipeline(false,ReadMode.CDC_ONLY,"latest",null),source("x",null)); var metadata = new Metadata(Map.of("label","value"),"changed display text");
        var next = inputs(pipeline(false,ReadMode.CDC_ONLY,"latest",metadata),source("x",metadata));
        assertThat(next).isNotEqualTo(old); assertThat(next.sameRecovery(old)).isTrue();
    }
    @Test void sourceSwitchReadModeStartAndSelectionArePartOfTheCapturedContract() {
        var source = source("x",null); var old = inputs(pipeline(false,ReadMode.CDC_ONLY,"latest",null),source);
        assertThat(inputs(pipeline(true,ReadMode.CDC_ONLY,"latest",null),source).sameRecovery(old)).isFalse();
        assertThat(inputs(pipeline(false,ReadMode.SNAPSHOT_ONLY,"latest",null),source).sameRecovery(old)).isFalse();
        assertThat(inputs(pipeline(false,ReadMode.SNAPSHOT_AND_CDC,"latest",null),source).sameRecovery(old)).isFalse();
        assertThat(inputs(pipeline(false,ReadMode.CDC_ONLY,"earliest",null),source).sameRecovery(old)).isFalse();
        assertThat(PipelinePreparationInputs.of(SCOPE,pipeline(false,ReadMode.CDC_ONLY,"latest",null),Map.of("input",source),Map.of("input",Set.of())).sameRecovery(old)).isFalse();
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings={"not-an-instant","+1000000000-12-31T23:59:59.999999999Z","at:2026-10-19T00:00:00Z"})
    void invalidStartsAreCodedBeforeAFirstCreationInputCanExist(String start) {
        assertThatThrownBy(() -> inputs(pipeline(false,ReadMode.CDC_ONLY,start,null),source("x",null)))
                .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,failure -> {
                    assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_INVALID);
                    assertThat(failure.args()).containsEntry("id","flow").containsEntry("field","settings.start_from");
                    assertThat(failure.getCause()).isNull();
                });
    }
    @Test void retryCallbacksCannotObserveCallerMutationsOfResolvedSelections() {
        var selected = new HashSet<>(Set.of("orders")); var selections = new HashMap<String,Set<String>>(); selections.put("input",selected);
        var frozen = PipelinePreparationInputs.of(SCOPE,pipeline(false,ReadMode.CDC_ONLY,"latest",null),Map.of("input",source("x",null)),selections);
        selected.add("late"); selections.clear();
        assertThat(frozen.selectedTables().get("input")).containsExactly("orders"); assertThat(frozen.directChannels().getFirst().tables()).containsExactly("orders");
        assertThatThrownBy(() -> frozen.selectedTables().get("input").clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> frozen.directChannels().clear()).isInstanceOf(UnsupportedOperationException.class);
    }
}
