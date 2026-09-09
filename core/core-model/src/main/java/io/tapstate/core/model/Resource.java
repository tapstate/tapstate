package io.tapstate.core.model;

import java.util.Map;

/**
 * A top-level {@code .tap.yml} resource. One document holds exactly one resource;
 * identity is the top-level {@code id} (§2, F6).
 *
 * <p>The model represents the {@code tapstate/v1} grammar only — {@code version} is a
 * constant of the contract, not a field of the model.
 */
@Doc("A top-level tapstate/v1 resource; one document holds exactly one of source, pipeline, transform, view or serve.")
public sealed interface Resource
        permits SourceResource, PipelineResource, TransformResource, ViewResource, ServeResource {

    String VERSION = "tapstate/v1";

    /** Resource kind discriminator as it appears in YAML ({@code kind:}). */
    String kind();

    /** Top-level id — unique per workspace across all kinds (F8). */
    String id();

    /** Optional annotation block (labels / description); never identity. */
    Metadata metadata();

    /** Experimental escape hatch (§11.6); exempt from the v1 freeze. */
    Map<String, Object> experimental();
}
