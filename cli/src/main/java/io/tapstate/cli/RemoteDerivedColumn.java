package io.tapstate.cli;

/**
 * One output column across the three sides a reader compares: what the step was recorded producing,
 * what it produces now, and what the target table actually declares.
 *
 * <p>Each side is nullable, and null means that side does not have the column at all - which is the
 * answer for a column that has appeared or gone, and is why it is not flattened into an empty string.
 * The target side is the target's own declaration in its own words ({@code varchar(50)}), because
 * whether the new values fit is decided by the width the target declares and the shared type
 * vocabulary has thrown that away by the time it says {@code STRING}.
 *
 * <p>This mirrors the server's shape independently (rule R6: the CLI carries no shared control type).
 */
record RemoteDerivedColumn(String column, String recorded, String derived, String target) {
}
