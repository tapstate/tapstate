package io.tapstate.app;

import io.tapstate.core.alerting.AlarmDiagnostics;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class AlarmDiagnosticCaptureTest {
    @Test void defaultOmitsUnclassifiedMessagesButKeepsOriginalLocationsAndChain() {
        var cause=new IllegalArgumentException("password=never-upload-me");
        var failure=new IllegalStateException("mongodb://user:pass@host/db",cause);
        failure.setStackTrace(new StackTraceElement[]{new StackTraceElement("example.Pipeline","run","Pipeline.java",42)});
        var trace=AlarmDiagnosticCapture.capture(failure,List.of()).orElseThrow();
        assertThat(trace.text()).contains("IllegalStateException","\tat example.Pipeline.run(Pipeline.java:42)","Caused by: java.lang.IllegalArgumentException").doesNotContain("never-upload-me","mongodb://");
        assertThat(trace.redacted()).isTrue();assertThat(trace.exceptionNodes()).isEqualTo(2);
    }
    @Test void explicitMessagesAndEveryFrameFieldAreRedactedBeforeFormatting() {
        var failure=new IllegalStateException("password=hunter2 uri=mongodb://u:p@host/db custom=known-value");
        failure.setStackTrace(new StackTraceElement[]{new StackTraceElement("known-value.Type","m","known-value.java",7)});
        var trace=AlarmDiagnosticCapture.capture(failure,List.of("known-value"),t->Optional.of(t.getMessage())).orElseThrow();
        assertThat(trace.text()).contains("password=[redacted]","[redacted-uri]","[redacted].Type").doesNotContain("hunter2","known-value","u:p@host");
    }
    @Test void hostileFormattingMethodsAreNeverInvoked() {
        var failure=new RuntimeException(){@Override public String toString(){throw new AssertionError();}@Override public String getMessage(){throw new AssertionError();}@Override public String getLocalizedMessage(){throw new AssertionError();}};
        assertThat(AlarmDiagnosticCapture.capture(failure,List.of())).isPresent();
    }
    @Test void cyclesAndSuppressedExceptionsAreBounded() {
        var a=new RuntimeException();var b=new RuntimeException();var suppressed=new IllegalStateException();
        a.setStackTrace(new StackTraceElement[0]);b.setStackTrace(new StackTraceElement[0]);suppressed.setStackTrace(new StackTraceElement[0]);
        a.initCause(b);b.initCause(a);a.addSuppressed(suppressed);
        var trace=AlarmDiagnosticCapture.capture(a,List.of()).orElseThrow();
        assertThat(trace.text()).contains("Suppressed:","circular exception reference");assertThat(trace.exceptionNodes()).isEqualTo(3);
    }
    @Test void nodeFrameLineAndUtf8BudgetsHoldWithVisibleTruncation() {
        RuntimeException root=new RuntimeException(), current=root;
        for(int i=0;i<12;i++){current.setStackTrace(new StackTraceElement[0]);var next=new RuntimeException();current.initCause(next);current=next;}
        var trace=AlarmDiagnosticCapture.capture(root,List.of()).orElseThrow();assertThat(trace.exceptionNodes()).isEqualTo(8);assertThat(trace.truncated()).isTrue();
        var frames=new StackTraceElement[400];java.util.Arrays.fill(frames,new StackTraceElement("longName".repeat(90),"run","File.java",42));root=new RuntimeException();root.setStackTrace(frames);
        trace=AlarmDiagnosticCapture.capture(root,List.of()).orElseThrow();
        assertThat(trace.text().getBytes(java.nio.charset.StandardCharsets.UTF_8).length).isLessThanOrEqualTo(AlarmDiagnostics.MAX_TEXT_BYTES);
        assertThat(trace.text().lines().count()).isLessThanOrEqualTo(256);assertThat(trace.truncated()).isTrue();
    }
    @Test void captureFailureIsExplicitlyAbsentAndControlCharactersCannotForgeFrames() {
        var failure=new RuntimeException(){@Override public StackTraceElement[] getStackTrace(){throw new IllegalStateException("unsafe getter");}};
        assertThat(AlarmDiagnosticCapture.capture(failure,List.of())).isEmpty();
        var regular=new RuntimeException("line\n\tat forged.frame");regular.setStackTrace(new StackTraceElement[0]);
        var trace=AlarmDiagnosticCapture.capture(regular,List.of(),t->Optional.of(t.getMessage())).orElseThrow();
        assertThat(trace.text()).doesNotContain("\n");assertThat(trace.redacted()).isTrue();
        assertThat(AlarmDiagnosticCapture.capture(null,List.of())).isEmpty();
    }
}
