package io.tapstate.core.validation;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/** Stable candidate order only, never a substitute for local admission or shared ownership. */
public final class ValidationOwnerRanking {
    private ValidationOwnerRanking() {}

    /** Highest unsigned SHA-256 score first, with node ID as the deterministic tie breaker. */
    public static List<String> rank(String clusterId, String pipelineId, Collection<String> eligibleNodeIds) {
        ValidationError.identifier(clusterId, "clusterId");
        ValidationError.identifier(pipelineId, "pipelineId");
        if (eligibleNodeIds == null || eligibleNodeIds.size() > 1024) {
            throw ValidationError.invalid("eligibleNodeIds");
        }
        List<Candidate> candidates = new ArrayList<>(eligibleNodeIds.size());
        var seen = new HashSet<String>();
        for (String nodeId : eligibleNodeIds) {
            ValidationError.identifier(nodeId, "nodeId");
            if (!seen.add(nodeId)) throw ValidationError.invalid("duplicateNodeId");
            candidates.add(new Candidate(nodeId, score(clusterId, pipelineId, nodeId)));
        }
        candidates.sort((left, right) -> {
            int scoreOrder = Arrays.compareUnsigned(right.score(), left.score());
            return scoreOrder != 0 ? scoreOrder : left.nodeId().compareTo(right.nodeId());
        });
        return candidates.stream().map(Candidate::nodeId).toList();
    }

    private static byte[] score(String clusterId, String pipelineId, String nodeId) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String part : List.of("validation-owner-v1", clusterId, pipelineId, nodeId)) {
                byte[] bytes = part.getBytes(StandardCharsets.UTF_8);
                digest.update((byte) (bytes.length >>> 24));
                digest.update((byte) (bytes.length >>> 16));
                digest.update((byte) (bytes.length >>> 8));
                digest.update((byte) bytes.length);
                digest.update(bytes);
            }
            return digest.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is required by the Java platform", impossible);
        }
    }

    private record Candidate(String nodeId, byte[] score) {}
}
