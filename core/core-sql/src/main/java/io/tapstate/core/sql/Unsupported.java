package io.tapstate.core.sql;

/**
 * One SQL construct a join declaration may not be written with, and where it sits.
 *
 * <p>Carries no error code and no sentence anybody is meant to read. This module reports what it
 * found; the validation layer above owns the user-facing vocabulary and turns this into a coded
 * diagnostic. {@code shape} names the construct the way SQL names it, so it survives being placed
 * into a message in any language.
 *
 * @param shape  the construct, as SQL spells it -- {@code FULL OUTER JOIN}, {@code GROUP BY},
 *               {@code COUNT}
 * @param line   line the construct starts on, counting from 1 within the SQL text alone
 * @param column column the construct starts at, counting from 1
 */
public record Unsupported(String shape, int line, int column) {
}
