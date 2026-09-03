package io.tapstate.control.restapi;

import io.tapstate.control.core.ArtifactListEntry;

import java.util.List;

/**
 * The artifact-list response body: the stored artifacts a list read returns, each as its canonical form
 * from the truth layer. A wrapper object rather than a bare array so the response can grow fields
 * (paging, a total) without changing the wire shape.
 */
public record ArtifactList(List<ArtifactListEntry> artifacts) {
}
