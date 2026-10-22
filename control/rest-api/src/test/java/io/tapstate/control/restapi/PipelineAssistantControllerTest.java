package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineDraftError;
import io.tapstate.control.core.PipelineDraftService;
import io.tapstate.control.core.AiProviderClient;
import io.tapstate.control.core.AiProviderService;
import io.tapstate.control.core.AiProviderSettings;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.SourceProjectionService;
import io.tapstate.control.core.SourceRepresentation;
import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.PlanAdvisories;
import io.tapstate.control.core.SchemaDerivation;
import io.tapstate.control.core.OpenAiCompatibleClient;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AiProviderSettingsStore;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.AuditStore;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.http.MediaType;
import tools.jackson.databind.json.JsonMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import static org.assertj.core.api.Assertions.*;

class PipelineAssistantControllerTest {
    @Test void malformedMessageEntryIsRejectedWithCodedRequestErrorBeforeStreaming() {
        var store = new PipelineApiTest.FakePipelineDraftStore();
        store.create(draft());
        var providers = providerController();
        var beans = new DefaultListableBeanFactory();
        beans.registerSingleton("drafts", new PipelineDraftService(store));
        var controller = new PipelineAssistantController(beans.getBeanProvider(PipelineDraftService.class),
                beans.getBeanProvider(io.tapstate.control.core.SourceProjectionService.class),
                beans.getBeanProvider(io.tapstate.control.core.SourceSchemaQueryService.class), providers,
                new OpenAiCompatibleClient(),
                JsonMapper.builder().build());
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/pipelines/orders/draft:assistant");

        assertThatThrownBy(() -> controller.assistant("orders",
                Map.of("revision", 1, "messages", List.of("bad-entry")), request))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
        assertThatThrownBy(() -> controller.assistant("orders",
                Map.of("revision", 2, "messages", List.of(Map.of("role", "user", "text", "proposal"))), request))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(PipelineDraftError.REVISION_CONFLICT));
        assertThat(store.get("orders")).contains(draft());
    }

    @Test void blankBuilderCanHaveAUsefulAssistantConversationWithoutConfiguredSources() throws Exception {
        PipelineDraft configured = draft();
        PipelineDraft blank = new PipelineDraft("blank", 1, 1, PipelineDraft.Mode.WIZARD, "Blank", null, null,
                new PipelineDraft.Wizard(null, List.of(), List.of(), configured.wizard().output()),
                null, null, null, Instant.EPOCH, Instant.EPOCH, "author");
        var store = new PipelineApiTest.FakePipelineDraftStore();
        store.create(blank);
        var artifacts = new PipelineApiTest.FakeArtifactStore();
        var schemaStore = new SchemaStore() {
            public void save(DiscoveredSourceModel ignored) { throw new AssertionError("must not write schema"); }
            public Optional<DiscoveredSourceModel> get(String id) { return Optional.empty(); }
        };
        var audit = new AuditGate(new PipelineApiTest.RecordingAuditStore(), Clock.systemUTC());
        var services = new PipelineApiTest.TestApp();
        var apply = new ApplyService(TapstateCatalog::load, artifacts, audit, schemaStore,
                PlanAdvisories.none(), SchemaDerivation.none());
        var sources = new SourceProjectionService(apply,
                services.artifactQueryService(artifacts), services.artifactMutationService(artifacts, audit),
                new SourceRepresentation(TapstateCatalog::load));
        var beans = new DefaultListableBeanFactory();
        beans.registerSingleton("drafts", new PipelineDraftService(store));
        beans.registerSingleton("sources", sources);
        beans.registerSingleton("schemas", new io.tapstate.control.core.SourceSchemaQueryService(artifacts, schemaStore));
        AtomicReference<Map<String, Object>> sentContext = new AtomicReference<>();
        AiProviderClient fake = new AiProviderClient() {
            public void check(AiProviderSettings ignored) { }
            public Optional<Map<String, Object>> propose(AiProviderSettings settings, List<Map<String, String>> messages,
                    Map<String, Object> context, java.util.function.Consumer<String> text) {
                sentContext.set(context);
                text.accept("Connect a PostgreSQL or MySQL Source and discover its schema to draft a pipeline.");
                return Optional.empty();
            }
        };
        var controller = new PipelineAssistantController(beans.getBeanProvider(PipelineDraftService.class),
                beans.getBeanProvider(SourceProjectionService.class),
                beans.getBeanProvider(io.tapstate.control.core.SourceSchemaQueryService.class),
                providerController(fake), fake, JsonMapper.builder().build());
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/pipelines/blank/draft:assistant");
        var response = controller.assistant("blank", Map.of("revision", 1,
                "messages", List.of(Map.of("role", "user", "text", "Help me create a pipeline"))), request);
        var output = new ByteArrayOutputStream();
        response.getBody().writeTo(output);
        String stream = output.toString(StandardCharsets.UTF_8);

        assertThat(sentContext.get()).containsKey("currentWizard").containsEntry("sources", List.of())
                .containsEntry("proposalEnabled", false);
        assertThat(stream).contains("Connect a PostgreSQL or MySQL Source", "data-status", "complete",
                "\"finishReason\":\"stop\"")
                .doesNotContain("data-proposal", "control.malformed-request", "\\\"type\\\":\\\"error\\\"");
        assertThat(store.get("blank")).contains(blank);
    }

    @Test void mysqlProposalStreamIsValidatedAndReleasesCapacityAfterEachBody() throws Exception {
        verifyValidatedStream("mysql");
    }

    @Test void assistantAssignsAnIdWhenTheModelOmitsOrReturnsABlankOne() throws Exception {
        verifyValidatedStream("mysql", true);
    }

    @Test void postgresProposalStreamUsesOnlyItsDiscoveredSchema() throws Exception {
        verifyValidatedStream("postgres");
    }

    private static void verifyValidatedStream(String connector) throws Exception {
        verifyValidatedStream(connector, false);
    }

    private static void verifyValidatedStream(String connector, boolean missingTransformId) throws Exception {
        var store = new PipelineApiTest.FakePipelineDraftStore();
        PipelineDraft base = draft(); store.create(base);
        var artifacts = new PipelineApiTest.FakeArtifactStore();
        artifacts.seed("version: tapstate/v1\nkind: source\nid: src\nconnector: " + connector + "\nmode: cdc\nconfig: {}\n");
        artifacts.seed("version: tapstate/v1\nkind: source\nid: dst\nconnector: mongodb\nconfig: {}\n");
        var schema = new DiscoveredSourceModel("src", connector, 1,
                new io.tapstate.spi.store.SourceModel(List.of(new io.tapstate.spi.store.SourceTable("orders",
                        List.of(new io.tapstate.spi.store.SourceField("id", "integer")), List.of("id"), List.of()))));
        SchemaStore schemaStore = new SchemaStore() {
            public void save(DiscoveredSourceModel ignored) { throw new AssertionError("must not write schema"); }
            public Optional<DiscoveredSourceModel> get(String id) { return Optional.of(schema); }
        };
        var audit = new AuditGate(new PipelineApiTest.RecordingAuditStore(), Clock.systemUTC());
        var services = new PipelineApiTest.TestApp();
        var apply = new ApplyService(TapstateCatalog::load, artifacts, audit, schemaStore,
                PlanAdvisories.none(), SchemaDerivation.none());
        var sources = new SourceProjectionService(apply,
                services.artifactQueryService(artifacts), services.artifactMutationService(artifacts, audit),
                new SourceRepresentation(TapstateCatalog::load));
        var schemas = new io.tapstate.control.core.SourceSchemaQueryService(artifacts, schemaStore);
        var beans = new DefaultListableBeanFactory();
        var draftService = new PipelineDraftService(store, services.artifactQueryService(artifacts), audit, apply);
        beans.registerSingleton("drafts", draftService);
        beans.registerSingleton("sources", sources);
        beans.registerSingleton("schemas", schemas);
        var mapper = JsonMapper.builder().build();
        Map<String, Object> candidate = AssistantWizardJson.view(
                AssistantWizardJson.object(PipelineDraftJson.write(mapper, base).get("wizard")));
        candidate.put("destinationTable", "customer_orders");
        var candidateInput = new java.util.LinkedHashMap<>(PipelineDraftJson.write(mapper, base));
        candidateInput.put("wizard", AssistantWizardJson.wire(candidate,
                AssistantWizardJson.object(candidateInput.get("wizard"))));
        try {
            draftService.previewCandidate("orders", PipelineDraftJson.read(mapper, candidateInput,
                    "orders", 1, base.updatedBy()));
        } catch (TapstateException invalid) {
            throw new AssertionError(invalid.getMessage(), invalid);
        }
        AtomicReference<Map<String, Object>> sentContext = new AtomicReference<>();
        AtomicReference<Map<String, Object>> proposedCandidate = new AtomicReference<>(candidate);
        AtomicBoolean providerFailure = new AtomicBoolean();
        AtomicInteger providerCalls = new AtomicInteger();
        AiProviderClient fake = new AiProviderClient() {
            public void check(AiProviderSettings ignored) { }
            public Optional<Map<String, Object>> propose(AiProviderSettings settings, List<Map<String, String>> messages,
                    Map<String, Object> context, java.util.function.Consumer<String> text) {
                providerCalls.incrementAndGet();
                sentContext.set(context);
                text.accept("I used the selected schema.");
                if (providerFailure.get()) throw AiProviderService.error(ControlError.AI_INVALID_PROPOSAL);
                return Optional.of(proposedCandidate.get());
            }
        };
        var controller = new PipelineAssistantController(beans.getBeanProvider(PipelineDraftService.class),
                beans.getBeanProvider(SourceProjectionService.class),
                beans.getBeanProvider(io.tapstate.control.core.SourceSchemaQueryService.class),
                providerController(fake), fake, mapper);
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/pipelines/orders/draft:assistant");
        Map<String, Object> body = Map.of("revision", 1,
                "messages", List.of(Map.of("role", "user", "text", "Rename the destination")));
        var responses = new java.util.ArrayList<org.springframework.http.ResponseEntity<org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody>>();

        for (int index = 0; index < 5; index++) responses.add(controller.assistant("orders", body, request));

        assertThat(responses.getFirst().getHeaders().getContentType()).isNotNull();
        assertThat(responses.getFirst().getHeaders().getContentType().isCompatibleWith(MediaType.TEXT_EVENT_STREAM)).isTrue();
        assertThat(responses.getFirst().getHeaders().getFirst("x-vercel-ai-ui-message-stream")).isEqualTo("v1");
        for (int index = 0; index < responses.size(); index++) {
            var response = responses.get(index);
            var output = new ByteArrayOutputStream();
            response.getBody().writeTo(output);
            String stream = output.toString(StandardCharsets.UTF_8);
            assertThat(stream).as("completed assistant SSE stream").contains("data-proposal");
            assertThat(stream.indexOf("\"type\":\"start\"")).isLessThan(stream.indexOf("\"type\":\"data-proposal\""));
            assertThat(stream.indexOf("\"type\":\"data-proposal\"")).isLessThan(stream.indexOf("\"type\":\"finish\""));
            assertThat(stream).contains("I used the selected schema.", "customer_orders", "\"finishReason\":\"stop\"").doesNotContain("sk-test", "private-row");
            String fixturePath = System.getProperty("tapstate.assistant.stream-output");
            if (connector.equals("mysql") && index == 0 && fixturePath != null && !fixturePath.isBlank()) {
                Path fixture = Path.of(fixturePath);
                if (fixture.getParent() != null) Files.createDirectories(fixture.getParent());
                Files.write(fixture, output.toByteArray());
            }
        }

        if (missingTransformId) {
            for (boolean blankId : List.of(false, true)) {
                Map<String, Object> incompleteCandidate = new java.util.LinkedHashMap<>(candidate);
                Map<String, Object> incompleteRoot = new java.util.LinkedHashMap<>(
                        AssistantWizardJson.object(candidate.get("root")));
                Map<String, Object> transformInput = new java.util.LinkedHashMap<>();
                if (blankId) transformInput.put("id", "");
                transformInput.put("name", "filter");
                transformInput.put("type", "filter");
                transformInput.put("enabled", true);
                transformInput.put("config", Map.of("filter", Map.of("expr", "true")));
                incompleteRoot.put("transforms", List.of(transformInput));
                incompleteCandidate.put("root", incompleteRoot);
                proposedCandidate.set(incompleteCandidate);

                var missingIdOutput = new ByteArrayOutputStream();
                controller.assistant("orders", body, request).getBody().writeTo(missingIdOutput);
                String missingIdStream = missingIdOutput.toString(StandardCharsets.UTF_8);
                assertThat(missingIdStream).contains("data-proposal", "\"finishReason\":\"stop\"")
                        .doesNotContain(ControlError.AI_INVALID_PROPOSAL.code());

                Map<String, Object> validated = controller.validate("orders", 1, incompleteCandidate);
                Map<String, Object> normalized = AssistantWizardJson.object(validated.get("candidateWizard"));
                Map<String, Object> normalizedRoot = AssistantWizardJson.object(normalized.get("root"));
                Map<String, Object> transform = AssistantWizardJson.object(
                        AssistantWizardJson.list(normalizedRoot.get("transforms")).getFirst());
                assertThat(AssistantWizardJson.text(transform.get("id"))).isNotBlank();
            }
        }

        proposedCandidate.set(Map.of());
        var emptyCandidateResponse = controller.assistant("orders", body, request);
        var emptyCandidateOutput = new ByteArrayOutputStream();
        emptyCandidateResponse.getBody().writeTo(emptyCandidateOutput);
        String emptyCandidateStream = emptyCandidateOutput.toString(StandardCharsets.UTF_8);
        assertThat(emptyCandidateStream).contains(ControlError.AI_INVALID_PROPOSAL.code())
                .doesNotContain("\"state\":\"complete\"", "data-proposal");
        assertThat(store.get("orders")).contains(base);
        providerFailure.set(true);
        var failedOutput = new ByteArrayOutputStream();
        controller.assistant("orders", body, request).getBody().writeTo(failedOutput);
        String failedStream = failedOutput.toString(StandardCharsets.UTF_8);
        assertThat(failedStream).contains(ControlError.AI_INVALID_PROPOSAL.code(), "\"finishReason\":\"error\"")
                .doesNotContain("data-proposal", "customer_orders");
        providerFailure.set(false);

        AtomicInteger writes = new AtomicInteger();
        OutputStream disconnected = new OutputStream() {
            @Override public void write(int value) throws IOException { throw new IOException("disconnected"); }
            @Override public void write(byte[] bytes, int offset, int length) throws IOException {
                if (writes.incrementAndGet() == 5) throw new IOException("disconnected");
            }
        };
        controller.assistant("orders", body, request).getBody().writeTo(disconnected);
        assertThat(providerCalls).hasValue(8 + (missingTransformId ? 2 : 0));
        assertThat(failedStream).doesNotContain("sk-test");
        assertThat(store.get("orders")).contains(base);

        Map<String, Object> invalidExpressionCandidate = new java.util.LinkedHashMap<>(candidate);
        invalidExpressionCandidate.put("transforms", List.of(Map.of(
                "id", "assistant-filter", "name", "filter", "type", "filter", "enabled", true,
                "config", Map.of("filter", Map.of("expr", "afterr.region == 'US'")))));
        proposedCandidate.set(invalidExpressionCandidate);
        var invalidExpressionStreamOutput = new ByteArrayOutputStream();
        controller.assistant("orders", body, request).getBody().writeTo(invalidExpressionStreamOutput);
        String invalidExpressionStream = invalidExpressionStreamOutput.toString(StandardCharsets.UTF_8);
        assertThat(invalidExpressionStream).contains("dsl.illegal-expression", "transforms[0].expr", "\"finishReason\":\"error\"")
                .doesNotContain("data-proposal", "afterr.region");
        assertThat(invalidExpressionStream.indexOf("\"type\":\"data-status\""))
                .isLessThan(invalidExpressionStream.indexOf("\"type\":\"error\""));
        assertThat(store.get("orders")).contains(base);
        assertThat(sentContext.get()).containsKey("sources").containsKey("currentWizard");
        assertThat(sentContext.toString()).doesNotContain("sk-test", "private-row");
        assertThat(store.get("orders")).contains(base);

        Map<String, Object> invalidCandidate = new java.util.LinkedHashMap<>(candidate);
        Map<String, Object> invalidRoot = new java.util.LinkedHashMap<>(AssistantWizardJson.object(candidate.get("root")));
        invalidRoot.put("table", "not-in-the-discovered-schema");
        invalidCandidate.put("root", invalidRoot);
        assertThat(controller.validate("orders", 1, invalidCandidate)).containsEntry("valid", false)
                .doesNotContainKey("candidateWizard");
        assertThat(store.get("orders")).contains(base);
    }

    @Test void revisionConflictPrecedesParsingAndNeverWrites() {
        var store = new PipelineApiTest.FakePipelineDraftStore();
        var draft = draft(); store.create(draft);
        var controller = controller(new PipelineDraftService(store));
        assertThatThrownBy(() -> controller.validate("orders", 2, Map.of()))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(PipelineDraftError.REVISION_CONFLICT));
        assertThat(store.get("orders")).contains(draft);
    }
    @Test void incompleteCandidateIsNotApplicableAndDoesNotSave() {
        var store = new PipelineApiTest.FakePipelineDraftStore();
        var draft = draft(); store.create(draft);
        var result = controller(new PipelineDraftService(store)).validate("orders", 1, Map.of("root", Map.of()));
        assertThat(result).containsEntry("valid", false).doesNotContainKey("candidateWizard");
        assertThat(store.get("orders")).contains(draft);
    }

    @Test void malformedWizardShapeIsReturnedAsAnInvalidProposal() {
        var store = new PipelineApiTest.FakePipelineDraftStore();
        var draft = draft(); store.create(draft);
        var mapper = JsonMapper.builder().build();
        Map<String, Object> candidate = AssistantWizardJson.view(
                AssistantWizardJson.object(PipelineDraftJson.write(mapper, draft).get("wizard")));
        Map<String, Object> root = new java.util.LinkedHashMap<>(AssistantWizardJson.object(candidate.get("root")));
        root.put("columns", "not-an-array");
        candidate.put("root", root);

        Map<String, Object> result = controller(new PipelineDraftService(store)).validate("orders", 1, candidate);

        assertThat(result).containsEntry("valid", false).doesNotContainKey("candidateWizard");
        List<?> diagnostics = (List<?>) AssistantWizardJson.object(result.get("validation")).get("diagnostics");
        assertThat(AssistantWizardJson.object(diagnostics.getFirst()))
                .containsEntry("code", ControlError.AI_INVALID_PROPOSAL.code());
    }

    @Test void validCandidateGetsServerDiffWithoutSavingOrPublishing() {
        var store = new PipelineApiTest.FakePipelineDraftStore(); var base = draft(); store.create(base);
        var artifacts = new PipelineApiTest.FakeArtifactStore();
        artifacts.seed("version: tapstate/v1\nkind: source\nid: src\nconnector: mysql\nconfig: {}\n");
        var schema = new io.tapstate.spi.store.DiscoveredSourceModel("src", "mysql", 1,
                new io.tapstate.spi.store.SourceModel(List.of(new io.tapstate.spi.store.SourceTable("orders",
                        List.of(new io.tapstate.spi.store.SourceField("id", "integer"),
                                new io.tapstate.spi.store.SourceField("Db", "string")), List.of("id"), List.of()))));
        var schemaStore = new io.tapstate.spi.store.SchemaStore() {
            public void save(io.tapstate.spi.store.DiscoveredSourceModel ignored) { throw new AssertionError("must not write schema"); }
            public java.util.Optional<io.tapstate.spi.store.DiscoveredSourceModel> get(String id) { return java.util.Optional.of(schema); }
        };
        var beans = new DefaultListableBeanFactory();
        beans.registerSingleton("drafts", new PipelineDraftService(store));
        beans.registerSingleton("schemas", new io.tapstate.control.core.SourceSchemaQueryService(artifacts, schemaStore));
        var mapper = JsonMapper.builder().build();
        var controller = new PipelineAssistantController(beans.getBeanProvider(PipelineDraftService.class),
                beans.getBeanProvider(io.tapstate.control.core.SourceProjectionService.class),
                beans.getBeanProvider(io.tapstate.control.core.SourceSchemaQueryService.class), null,
                new OpenAiCompatibleClient(), mapper);
        Map<String, Object> view = AssistantWizardJson.view(AssistantWizardJson.object(PipelineDraftJson.write(mapper, base).get("wizard")));
        view.put("destinationTable", "changed_orders");
        var result = controller.validate("orders", 1, view);
        assertThat(result).containsEntry("valid", true).containsKey("candidateWizard");
        assertThat(AssistantWizardJson.object(result.get("diff")).get("after").toString()).contains("changed_orders");
        assertThat(store.get("orders")).contains(base);

        for (String arrayField : List.of("relations", "transforms")) {
            Map<String, Object> nullArrayCandidate = new java.util.LinkedHashMap<>(view);
            nullArrayCandidate.put(arrayField, null);
            Map<String, Object> invalidArray = controller.validate("orders", 1, nullArrayCandidate);
            assertThat(invalidArray).containsEntry("valid", false).doesNotContainKey("candidateWizard");
            List<?> arrayDiagnostics = (List<?>) AssistantWizardJson.object(invalidArray.get("validation")).get("diagnostics");
            assertThat(arrayDiagnostics).as("null %s must not normalize into an empty array", arrayField).hasSize(1);
            assertThat(AssistantWizardJson.object(arrayDiagnostics.getFirst()))
                    .containsEntry("code", ControlError.AI_INVALID_PROPOSAL.code());
        }

        Map<String, Object> nullRootTransforms = new java.util.LinkedHashMap<>(view);
        Map<String, Object> nullRoot = new java.util.LinkedHashMap<>(AssistantWizardJson.object(view.get("root")));
        nullRoot.put("transforms", null);
        nullRootTransforms.put("root", nullRoot);
        assertThat(controller.validate("orders", 1, nullRootTransforms))
                .containsEntry("valid", false).doesNotContainKey("candidateWizard");

        Map<String, Object> relation = new java.util.LinkedHashMap<>(Map.ofEntries(
                Map.entry("source", "src"), Map.entry("table", "orders"),
                Map.entry("type", "flatten"), Map.entry("primaryKey", List.of("id")), Map.entry("arrayKey", List.of()),
                Map.entry("targetField", ""), Map.entry("key", "id"), Map.entry("parentKey", "id"),
                Map.entry("parentTable", "orders"), Map.entry("preProcessing", List.of()),
                Map.entry("conditions", List.of(Map.of("childKey", "id", "parentKey", "id"))),
                Map.entry("transforms", List.of()), Map.entry("children", List.of())));
        Map<String, Object> relationCandidate = new java.util.LinkedHashMap<>(view);
        relationCandidate.put("relations", List.of(relation));
        Map<String, Object> relationResult = controller.validate("orders", 1, relationCandidate);
        assertThat(relationResult).containsEntry("valid", true);
        Map<String, Object> normalizedRelation = AssistantWizardJson.object(
                AssistantWizardJson.list(AssistantWizardJson.object(relationResult.get("candidateWizard")).get("relations"))
                        .getFirst());
        assertThat(AssistantWizardJson.text(normalizedRelation.get("id"))).isNotBlank();
        for (String arrayField : List.of("transforms", "children")) {
            Map<String, Object> nullRelationArray = new java.util.LinkedHashMap<>(relation);
            nullRelationArray.put(arrayField, null);
            Map<String, Object> nullRelationCandidate = new java.util.LinkedHashMap<>(view);
            nullRelationCandidate.put("relations", List.of(nullRelationArray));
            assertThat(controller.validate("orders", 1, nullRelationCandidate))
                    .as("null relation %s must not be normalized into an empty array", arrayField)
                    .containsEntry("valid", false).doesNotContainKey("candidateWizard");
        }

        Map<String, Object> mapTransform = new java.util.LinkedHashMap<>(Map.of("id", "assistant-map", "name", "map",
                "type", "map", "enabled", true, "config", Map.of("map", Map.of("fields", List.of(Map.of(
                        "id", "name-rule", "outputField", "name", "ruleType", "rename", "value", "$Db")),
                        "passthroughUnlisted", true))));
        Map<String, Object> mapCandidate = new java.util.LinkedHashMap<>(view);
        Map<String, Object> mappedRoot = new java.util.LinkedHashMap<>(AssistantWizardJson.object(view.get("root")));
        mappedRoot.put("transforms", List.of(mapTransform));
        mapCandidate.put("root", mappedRoot);
        Map<String, Object> mapResult = controller.validate("orders", 1, mapCandidate);
        assertThat(mapResult).containsEntry("valid", true);
        assertThat(AssistantWizardJson.object(mapResult.get("diff")).get("after").toString())
                .contains("name", "$Db");
        Map<String, Object> nullMapFields = new java.util.LinkedHashMap<>(view);
        Map<String, Object> invalidMapTransform = new java.util.LinkedHashMap<>(mapTransform);
        Map<String, Object> mapConfig = new java.util.LinkedHashMap<>(AssistantWizardJson.object(invalidMapTransform.get("config")));
        Map<String, Object> mapBody = new java.util.LinkedHashMap<>(AssistantWizardJson.object(mapConfig.get("map")));
        mapBody.put("fields", null);
        mapConfig.put("map", mapBody);
        invalidMapTransform.put("config", mapConfig);
        nullMapFields.put("transforms", List.of(invalidMapTransform));
        assertThat(controller.validate("orders", 1, nullMapFields))
                .containsEntry("valid", false).doesNotContainKey("candidateWizard");

        Map<String, Object> invalidExpressionCandidate = new java.util.LinkedHashMap<>(view);
        invalidExpressionCandidate.put("transforms", List.of(Map.of(
                "id", "assistant-filter", "name", "filter", "type", "filter", "enabled", true,
                "config", Map.of("filter", Map.of("expr", "afterr.region == 'US'")))));
        Map<String, Object> invalidExpression = controller.validate("orders", 1, invalidExpressionCandidate);
        assertThat(invalidExpression).containsEntry("valid", false).doesNotContainKey("candidateWizard");
        Map<String, Object> expressionValidation = AssistantWizardJson.object(invalidExpression.get("validation"));
        List<?> expressionDiagnostics = (List<?>) expressionValidation.get("diagnostics");
        assertThat(expressionDiagnostics).hasSize(1);
        Map<String, Object> expressionDiagnostic = AssistantWizardJson.object(expressionDiagnostics.getFirst());
        assertThat(expressionDiagnostic).containsEntry("code", "dsl.illegal-expression");
        assertThat(AssistantWizardJson.object(expressionDiagnostic.get("params")))
                .containsEntry("path", "transforms[0].expr")
                .doesNotContainKeys("expr", "detail");

        view.put("destinationSourceId", "unselected-destination");
        assertThat(controller.validate("orders", 1, view)).containsEntry("valid", false).doesNotContainKey("candidateWizard");
    }
    @Test void unfinishedDestinationStillAllowsAFilterProposalWithoutSaving() {
        var store = new PipelineApiTest.FakePipelineDraftStore();
        var configured = draft();
        var unfinished = new PipelineDraft(configured.pipelineId(), configured.schemaVersion(), configured.revision(),
                configured.mode(), configured.name(), configured.description(), configured.graph(),
                new PipelineDraft.Wizard(configured.wizard().root(), configured.wizard().related(),
                        configured.wizard().transforms(), new PipelineDraft.Output("atlas",
                                Map.of("sourceId", "", "table", "", "writeMode", "upsert"))),
                configured.baseArtifactHash(), configured.publishedDraftRevision(), configured.publishedArtifactHash(),
                configured.createdAt(), configured.updatedAt(), configured.updatedBy());
        store.create(unfinished);
        var beans = new DefaultListableBeanFactory();
        beans.registerSingleton("drafts", new PipelineDraftService(store));
        var mapper = JsonMapper.builder().build();
        var controller = new PipelineAssistantController(beans.getBeanProvider(PipelineDraftService.class),
                beans.getBeanProvider(io.tapstate.control.core.SourceProjectionService.class),
                beans.getBeanProvider(io.tapstate.control.core.SourceSchemaQueryService.class), null,
                new OpenAiCompatibleClient(), mapper);
        Map<String, Object> view = AssistantWizardJson.view(AssistantWizardJson.object(
                PipelineDraftJson.write(mapper, unfinished).get("wizard")));
        Map<String, Object> root = new java.util.LinkedHashMap<>(AssistantWizardJson.object(view.get("root")));
        root.put("transforms", List.of(Map.of("id", "assistant-filter", "name", "filter", "type", "filter",
                "enabled", true, "config", Map.of("filter", Map.of("expr", "after.User == 'root'")))));
        view.put("root", root);
        var input = new java.util.LinkedHashMap<>(PipelineDraftJson.write(mapper, unfinished));
        input.put("wizard", AssistantWizardJson.wire(view, AssistantWizardJson.object(input.get("wizard"))));
        assertThat(new PipelineDraftService(store).previewCandidate("orders", PipelineDraftJson.read(mapper,
                input, "orders", 1, unfinished.updatedBy())).serve()).isNull();
        assertThat(store.get("orders")).contains(unfinished);
    }
    @Test void focusProjectsOnlyKnownBuilderContextAndNeverArbitraryData() {
        assertThat(PipelineAssistantController.focus(draft(), Map.of("kind", "root", "nodeId", "root", "rows", List.of("private-row"))))
                .containsExactlyInAnyOrderEntriesOf(Map.of("kind", "root", "nodeId", "root"));
        assertThat(PipelineAssistantController.focus(draft(), Map.of("kind", "arbitrary", "nodeId", "secret"))).isEmpty();
        assertThat(PipelineAssistantController.focus(draft(), "root")).isEmpty();
    }
    @Test void missingDraftCannotProduceAProposal() {
        var controller = controller(new PipelineDraftService(new PipelineApiTest.FakePipelineDraftStore()));
        assertThatThrownBy(() -> controller.validate("orders", 1, Map.of()))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(PipelineDraftError.NOT_FOUND));
    }
    private static PipelineDraft draft() {
        return new PipelineDraft("orders", 1, 1, PipelineDraft.Mode.WIZARD, "Orders", null, null,
                new PipelineDraft.Wizard(new PipelineDraft.Root("root", "src", "orders", List.of("id"), List.of()),
                        List.of(), List.of(), new PipelineDraft.Output("atlas", Map.of("sourceId", "dst", "table", "orders", "writeMode", "upsert"))),
                null, null, null, Instant.EPOCH, Instant.EPOCH, "author");
    }
    private static PipelineAssistantController controller(PipelineDraftService service) {
        var beans = new DefaultListableBeanFactory(); beans.registerSingleton("drafts", service);
        return new PipelineAssistantController(beans.getBeanProvider(PipelineDraftService.class),
                beans.getBeanProvider(io.tapstate.control.core.SourceProjectionService.class),
                beans.getBeanProvider(io.tapstate.control.core.SourceSchemaQueryService.class), null,
                new OpenAiCompatibleClient(), JsonMapper.builder().build());
    }

    private static AiProviderController providerController() {
        return providerController(new OpenAiCompatibleClient());
    }

    private static AiProviderController providerController(AiProviderClient client) {
        AiProviderSettingsStore store = new AiProviderSettingsStore() {
            public java.util.Optional<Settings> get(String owner) {
                return java.util.Optional.of(new Settings("openai-compatible", "https://api.openai.com/v1", "model", "sk-test"));
            }
            public void put(String owner, Settings settings) { }
            public void delete(String owner) { }
        };
        var audit = new AuditStore() {
            public void record(io.tapstate.spi.store.AuditRecord record) { }
            public List<io.tapstate.spi.store.AuditRecord> query(Instant since, int limit) { return List.of(); }
        };
        AiProviderService service = new AiProviderService(store, new AuditGate(audit, Clock.systemUTC()));
        var beans = new DefaultListableBeanFactory();
        beans.registerSingleton("mode", AuthenticationMode.CLOUD);
        return new AiProviderController(null, null, beans.getBeanProvider(AuthenticationMode.class), client) {
            @Override String owner(HttpServletRequest request) { return "workspace"; }
            @Override AiProviderService service() { return service; }
        };
    }
}
