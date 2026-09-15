package io.tapstate.core.logging;

/**
 * The slot a log line carries its pipeline attribution in.
 *
 * <p>A line is filed against a pipeline by the thread that writes it, and the thread says which pipeline
 * by putting its id in this slot of the logging backend's diagnostic context. Three places have to agree
 * on the name: whoever sets it while driving a pipeline, the appender that files lines by it, and the
 * log format that prints it. The first two read it from here. The third cannot -- a logging configuration
 * is not code and spells the name itself -- so a change here is a change in that file too, and the
 * operational-logging cases assert the pair.
 *
 * <p>An unattributed line is not an error: work that belongs to no single pipeline (the process starting,
 * a schema discovery a source may be asked for by any number of pipelines) leaves the slot empty, and the
 * read face holds only the lines that named one. Attributing those to a pipeline would be inventing an
 * answer, not filling in a blank.
 */
public final class PipelineAttribution {

    /** The diagnostic-context key a log line's pipeline id is carried in. */
    public static final String MDC_KEY = "pipeline_id";

    private PipelineAttribution() {
    }
}
