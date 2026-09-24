package io.tapstate.core.model;

/**
 * Error policy (§1, X7): override chain = step explicit > settings task-level >
 * system default fail. DLQ physical landing is open (F10).
 */
@Doc("How the pipeline handles a record that fails processing.")
public enum ErrorPolicy {
    @Doc("Route the failed record to a dead-letter sink for later inspection and continue.")
    DEAD_LETTER("dead_letter"),
    @Doc("Drop the failed record and continue processing.")
    SKIP("skip"),
    @Doc("Stop the pipeline and report the failure.")
    FAIL("fail");

    private final String yaml;

    ErrorPolicy(String yaml) {
        this.yaml = yaml;
    }

    public String yaml() {
        return yaml;
    }
}
