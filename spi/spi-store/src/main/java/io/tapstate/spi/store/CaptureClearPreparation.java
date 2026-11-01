package io.tapstate.spi.store;

/** Original explicit-clear authority; it is neither first creation nor shared-loss recovery authority. */
public interface CaptureClearPreparation {
    String operationId();
    long sequence();
    PipelineIntentScope originalScope();
    String originalOperationId();
    SharedSrsRecoverySnapshot observation();
    CapturePreparation replacement();
    RequiredNoteScope notes();
    String requestHash();
    java.util.List<CaptureRetirementMember> members();
    void requireCurrent();
}
