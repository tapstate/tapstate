package io.tapstate.control.restapi;

import io.tapstate.spi.store.PipelineDraft;
import java.util.Map;
import tools.jackson.databind.json.JsonMapper;

/** Uses the actual Builder wire format in runtime smoke tests. */
public final class AssistantProposalFixture {
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private AssistantProposalFixture() { }

    public static Map<String, Object> view(PipelineDraft base) {
        return AssistantWizardJson.view(AssistantWizardJson.object(PipelineDraftJson.write(JSON, base).get("wizard")));
    }

    public static PipelineDraft readCandidate(PipelineDraft base, Map<String, Object> proposal) {
        AssistantWizardJson.validateViewShape(proposal);
        var encoded = PipelineDraftJson.write(JSON, base);
        var original = AssistantWizardJson.object(encoded.get("wizard"));
        encoded.put("wizard", AssistantWizardJson.wire(proposal, original));
        return PipelineDraftJson.read(JSON, encoded, base.pipelineId(), base.revision(), base.updatedBy());
    }
}
