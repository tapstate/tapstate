package io.tapstate.spi.store;

/** The fixed physical member identity observed by an original explicit clear. */
public record CaptureRetirementMember(String nodeId,String bootId,String memberId) {
    public CaptureRetirementMember {
        for(var identity:new String[]{nodeId,bootId,memberId})
            if(identity==null || identity.isBlank() || identity.getBytes(java.nio.charset.StandardCharsets.UTF_8).length>256)
                throw new IllegalArgumentException("invalid capture retirement member");
    }
}
