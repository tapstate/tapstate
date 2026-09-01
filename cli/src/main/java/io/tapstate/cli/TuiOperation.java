package io.tapstate.cli;

import java.util.Objects;

/** Pure lifecycle state for one TUI operation and its Ctrl-C policy. */
record TuiOperation(String id, String description, Kind kind, Status status) {

    enum Kind {
        COMMAND,
        STREAM,
        WRITE
    }

    enum Status {
        RUNNING,
        CANCELLATION_REQUESTED,
        SUBMITTED,
        WAITING_STOPPED,
        COMPLETED,
        FAILED,
        CANCELLED
    }

    enum CtrlCAction {
        REQUEST_CANCELLATION,
        STOP_WAITING,
        NOOP
    }

    record CtrlCResult(CtrlCAction action, TuiOperation operation,
                      boolean remoteCancellationRequested, String message) {
        CtrlCResult {
            action = Objects.requireNonNull(action, "action is required");
            operation = Objects.requireNonNull(operation, "operation is required");
            message = message == null ? "" : message;
        }
    }

    TuiOperation {
        id = required(id, "operation id");
        description = required(description, "operation description");
        kind = Objects.requireNonNull(kind, "operation kind");
        status = Objects.requireNonNull(status, "operation status");
    }

    static TuiOperation stream(String id, String description) {
        return new TuiOperation(id, description, Kind.STREAM, Status.RUNNING);
    }

    static TuiOperation command(String id, String description) {
        return new TuiOperation(id, description, Kind.COMMAND, Status.RUNNING);
    }

    static TuiOperation submittedWrite(String id, String description) {
        return new TuiOperation(id, description, Kind.WRITE, Status.SUBMITTED);
    }

    static TuiOperation write(String id, String description) {
        return new TuiOperation(id, description, Kind.WRITE, Status.RUNNING);
    }

    boolean cancellable() {
        return kind == Kind.STREAM && status == Status.RUNNING;
    }

    boolean submittedWrite() {
        return kind == Kind.WRITE && status == Status.SUBMITTED;
    }

    CtrlCResult onCtrlC() {
        if (cancellable()) {
            TuiOperation next = withStatus(Status.CANCELLATION_REQUESTED);
            return new CtrlCResult(CtrlCAction.REQUEST_CANCELLATION, next, true,
                    "cancellation requested for " + id);
        }
        if (submittedWrite()) {
            TuiOperation next = withStatus(Status.WAITING_STOPPED);
            return new CtrlCResult(CtrlCAction.STOP_WAITING, next, false,
                    "stopped waiting for submitted write " + id + "; outcome may still be in progress");
        }
        return new CtrlCResult(CtrlCAction.NOOP, this, false, "");
    }

    TuiOperation complete() {
        return withStatus(Status.COMPLETED);
    }

    TuiOperation failed() {
        return withStatus(Status.FAILED);
    }

    private TuiOperation withStatus(Status nextStatus) {
        return new TuiOperation(id, description, kind, nextStatus);
    }

    private static String required(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }
}
