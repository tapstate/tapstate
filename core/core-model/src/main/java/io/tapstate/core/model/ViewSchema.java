package io.tapstate.core.model;

/** View schema policy (§7): enforcement flag + evolution mode (e.g. additive). */
@Doc("Schema policy for a view: whether the schema is enforced and how it is allowed to evolve.")
public record ViewSchema(
        @Doc("Whether the declared view schema is strictly enforced at runtime.")
        Boolean enforce,
        @Doc("How the view schema is allowed to evolve over time, such as additive-only.")
        String evolution) {
}
