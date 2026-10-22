package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The "no audit, no execute" gate every audited operation passes through. An operation the registry
 * marks {@code audited} (a write / admin verb that mutates persisted control-plane state) must leave
 * an audit record before it runs: the gate writes the record first, and if that write fails the
 * operation is refused — it never runs — with a coded {@code control.audit-blocked} diagnostic that
 * carries the store failure as its cause. An operation with no audit flag (a read / list / probe)
 * runs straight through and leaves no record.
 *
 * <p>This is the audit-first form of the guarantee: the record is written before the action, so a
 * failed audit provably precedes any effect. The stronger single-transaction form — where an
 * artifact write and its audit record commit together — attaches where the artifact write itself
 * lands; either form upholds the same invariant that an unaudited operation does not execute.
 */
public final class AuditGate {

    private final AuditStore auditStore;
    private final Clock clock;

    public AuditGate(AuditStore auditStore, Clock clock) {
        this.auditStore = Objects.requireNonNull(auditStore, "auditStore");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Runs {@code action} under the audit gate. When {@code op} is audited, an audit record is written
     * first; a write failure refuses the operation ({@code control.audit-blocked}) and {@code action}
     * never runs. When {@code op} is not audited, {@code action} runs directly with no record.
     */
    public <T> T dispatch(Operation op, AuditContext ctx, Supplier<T> action) {
        Objects.requireNonNull(ctx, "ctx");
        return dispatchAll(op, List.of(ctx), action);
    }

    /**
     * Runs {@code action} once under the audit gate for a batch that touches several resources. When
     * {@code op} is audited, one record per context is written first — each resource the batch touches
     * attributed by its own id — and only then does {@code action} run, once; a write failure on any of
     * them refuses the whole batch ({@code control.audit-blocked}) and {@code action} never runs. An
     * empty context list is a batch that touches nothing: it leaves no record and runs straight through,
     * as does a non-audited operation.
     *
     * <p>The action stays a single call so a batch that lands as one atomic store write keeps that
     * atomicity: the records fan out per resource, the effect does not.
     */
    public <T> T dispatchAll(Operation op, List<AuditContext> contexts, Supplier<T> action) {
        Objects.requireNonNull(op, "op");
        Objects.requireNonNull(contexts, "contexts");
        Objects.requireNonNull(action, "action");
        if (op.audited()) {
            for (AuditContext ctx : contexts) {
                AuditRecord record = new AuditRecord(
                        clock.instant(), ctx.principal(), op.id(), ctx.resourceId(),
                        ctx.expectedContentHash(), ctx.detail());
                try {
                    auditStore.record(record);
                } catch (RuntimeException cause) {
                    throw new TapstateException(ControlError.AUDIT_BLOCKED, Map.of("op", op.id()), cause);
                }
            }
        }
        return action.get();
    }
}
