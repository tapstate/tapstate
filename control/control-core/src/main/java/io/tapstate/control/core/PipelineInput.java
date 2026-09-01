package io.tapstate.control.core;

import io.tapstate.core.model.Metadata;

import java.util.List;
import java.util.Map;

/** Structured JSON input for creating or replacing a Pipeline artifact. */
public record PipelineInput(
        String id,
        Metadata metadata,
        List<String> sources,
        List<Map<String, Object>> transforms,
        Map<String, Object> view,
        Map<String, Object> serve,
        Map<String, Object> settings,
        Map<String, Object> experimental) {

    public PipelineInput {
        sources = sources == null ? null : List.copyOf(sources);
        transforms = transforms == null ? null : List.copyOf(transforms);
        view = copy(view);
        serve = copy(serve);
        settings = copy(settings);
        experimental = copy(experimental);
    }

    private static Map<String, Object> copy(Map<String, Object> value) {
        return SourceDraft.copyJsonMap(value, true);
    }
}
