package io.tapstate.app;

import io.tapstate.control.core.PipelineIncarnationService;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.Resource;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.StartDeferred;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.PreExecutionFailure;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PreExecutionLifecycleInputTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void deletionOrRecreationBeforeSnapshotIsDeferredWithoutBorrowingItsInputs(boolean recreated) {
        Resource pipeline = new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: orders
                source: src_x
                view:
                  from: src_x
                  primary_key: id
                """);
        AtomicReference<String> incarnation = new AtomicReference<>("inc-old");
        ArtifactStore artifacts = mock(ArtifactStore.class);
        when(artifacts.get("orders")).thenReturn(Optional.of(pipeline));
        when(artifacts.pipelineIncarnationId("orders")).thenAnswer(ignored -> Optional.ofNullable(incarnation.get()));
        when(artifacts.ensurePipelineIncarnationId(anyString(), anyString())).thenAnswer(ignored -> Optional.of("inc-old"));
        ExecutionGenerationStore generations = mock(ExecutionGenerationStore.class);
        when(generations.currentGeneration("single", "orders")).thenReturn(OptionalLong.empty());
        DagSource source = mock(DagSource.class);
        doAnswer(invocation -> {
            incarnation.set(recreated ? "inc-new" : null);
            ArtifactStore snapshot = mock(ArtifactStore.class);
            when(snapshot.get("orders")).thenReturn(recreated ? Optional.of(pipeline) : Optional.empty());
            when(snapshot.list()).thenReturn(recreated ? List.of(pipeline) : List.of());
            Consumer<ArtifactStore> captured = invocation.getArgument(2);
            captured.accept(snapshot);
            throw new AssertionError("obsolete input snapshot reached validation");
        }).when(source).prepareStart(anyString(), anyString(), any());
        NestStateTeardown teardown = mock(NestStateTeardown.class);
        when(teardown.defaultDatabase()).thenReturn("default");
        EngineLifecycleActuator actuator = new EngineLifecycleActuator(mock(Engine.class), source,
                mock(PipelineCaptureCoordinator.class), teardown, PipelineActuationOwnership.single("single", generations),
                new PipelineIncarnationService(artifacts), new ObservationScopeRegistry(), null, null, generations);
        AtomicReference<PreExecutionFailure.Attempt> received = new AtomicReference<>();
        assertThatThrownBy(() -> actuator.prepareStart("orders", new DesiredState("orders", PipelineState.RUNNING, "revision"),
                new CheckpointDoc("orders", StateJson.of(PipelineState.NEW), 0L, Instant.EPOCH), received::set))
                .isInstanceOf(StartDeferred.class);
        assertThat(received.get()).isNull();
    }
}
