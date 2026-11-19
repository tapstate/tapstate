package io.tapstate.control.core;

/** Shares proposal prose handling across provider protocols while leaving Wizard arguments intact. */
final class AssistantProviderText {
    private static final int MAX_OPENING = 160;
    private enum Channel { REPLY, EXPLANATION }

    private final AiProviderClient.Output output;
    private final AssistantReplyLanguage language;
    private final boolean replyAlreadyShown;
    private final boolean reasoningAlreadyShown;
    private final StringBuilder opening = new StringBuilder();
    private Channel channel;
    private boolean checking;
    private boolean mismatch;
    private boolean replyShown;
    private boolean reasoningShown;
    private boolean emitted;

    AssistantProviderText(AiProviderClient.Output output, AssistantReplyLanguage language, AssistantProviderText previous) {
        this.output = output;
        this.language = language;
        checking = language != null;
        replyAlreadyShown = previous != null && previous.replyShown;
        reasoningAlreadyShown = previous != null && previous.reasoningShown;
        if (replyAlreadyShown) channel = Channel.REPLY;
    }

    void reply(String delta) {
        if (delta.isEmpty() || mismatch || replyAlreadyShown) return;
        if (!opening.isEmpty()) finishExplanation();
        if (mismatch) return;
        replyShown = true;
        emit(delta, Channel.REPLY);
    }

    void explanation(String delta) {
        if (delta.isEmpty() || mismatch) return;
        if (!checking) {
            emit(delta, Channel.EXPLANATION);
            return;
        }
        int length = Math.min(MAX_OPENING - opening.length(), delta.length());
        if (length < delta.length() && Character.isHighSurrogate(delta.charAt(length - 1))) length++;
        opening.append(delta, 0, length);
        var detected = AssistantReplyLanguage.detect(opening, false);
        if (detected != null || opening.length() >= MAX_OPENING || opening.indexOf("\n") >= 0) {
            releaseOpening(detected);
            if (!mismatch && length < delta.length()) emit(delta.substring(length), Channel.EXPLANATION);
        }
    }

    void finishExplanation() {
        if (!opening.isEmpty()) releaseOpening(AssistantReplyLanguage.detect(opening, true));
    }

    private void releaseOpening(AssistantReplyLanguage detected) {
        checking = false;
        mismatch = detected != null && detected != language;
        if (!mismatch) emit(opening.toString(), Channel.EXPLANATION);
        opening.setLength(0);
    }

    void reasoning(AiProviderClient.Reasoning part) {
        if (reasoningAlreadyShown) return;
        emitted = true;
        reasoningShown = true;
        output.reasoning(part);
    }

    private void emit(String delta, Channel next) {
        if (channel != null && channel != next) output.accept("\n\n");
        channel = next;
        emitted = true;
        output.accept(delta);
    }

    boolean emitted() { return emitted; }
    boolean mismatch() { return mismatch; }
}
