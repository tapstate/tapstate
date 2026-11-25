package io.tapstate.ci;

import org.apache.maven.execution.MojoExecutionEvent;
import org.apache.maven.execution.MojoExecutionListener;

/** Read-only configured-plugin metadata; outcome observation remains with the event spy. */
public final class ConfiguredBuildInputListener implements MojoExecutionListener {
    private BuildInputSpy observer;
    @Override public void beforeMojoExecution(MojoExecutionEvent event) { observer.configured(event); }
    @Override public void afterMojoExecutionSuccess(MojoExecutionEvent event) { }
    @Override public void afterExecutionFailure(MojoExecutionEvent event) { }
}
