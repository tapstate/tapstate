package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.tapstate.core.validation.ValidationRootCheck.*;

/**
 * Checks actual parent/flat field contributions against other writers and declared embedded paths.
 * Embedded paths remain reserved when an object is omitted or an array is empty. Unknown relation
 * contents must be blocked separately, never supplied here as an empty field set. This check neither
 * renders a document nor validates target values or the model's configured path syntax.
 */
public final class ValidationNestFieldCheck {
    public enum FieldKind { PARENT, FLAT, EMBED_PATH }

    /** Row fields retain their literal name for diagnostics; dotted prefixes are used only for overlap. */
    public record Field(String owner, FieldKind kind, List<String> path) {
        public Field {
            owner = ValidationError.identifier(owner, "nestFields.owner");
            Objects.requireNonNull(kind, "kind");
            if (path == null || path.isEmpty() || path.size() > 32 || kind != FieldKind.EMBED_PATH && path.size() != 1) {
                throw ValidationError.invalid("nestFields.path");
            }
            for (String segment : path) {
                if (kind == FieldKind.EMBED_PATH) ValidationError.identifier(segment, "nestFields.path");
                else ValidationError.fieldName(segment, "nestFields.path");
            }
            path = List.copyOf(path);
        }
    }
    public record Limits(long bytes, long operations, int fields, int conflicts) {
        public Limits {
            if (bytes < 1 || bytes > 32L * 1024 * 1024 || operations < 1 || operations > 64L * 1024 * 1024
                    || fields < 1 || fields > 1024 || conflicts < 1 || conflicts > 1024) {
                throw ValidationError.invalid("nestFields.limits");
            }
        }
    }
    public record Conflict(Field contributor, Field occupied) {}
    public record Result(List<Field> uncontested, List<Conflict> conflicts) {
        public Result {
            uncontested = List.copyOf(uncontested);
            conflicts = List.copyOf(conflicts);
        }
    }
    private record Named(Field field, String dotted) {}
    private ValidationNestFieldCheck() {}

    /** The caller supplies complete current field sets and all declared sibling paths for this parent. */
    public static Result check(ValidationObservation parent, List<Field> fields, Limits limits, Builder root) {
        Objects.requireNonNull(root, "root");
        try {
            Objects.requireNonNull(parent, "parent");
            requireScope(parent, root.observation());
            Result result = inspect(fields, limits);
            Set<Finding> findings = new HashSet<>();
            for (Conflict conflict : result.conflicts()) {
                Location at = location(parent, conflict.contributor());
                Finding cause = new Finding(Kind.SOURCE_FIELD_CONFLICT, at);
                if (findings.add(cause)) root.finding(cause);
                block(root, at, cause);
                block(root, location(parent, conflict.occupied()), cause);
            }
            return result;
        } catch (RuntimeException | Error failure) {
            root.close();
            throw failure;
        }
    }

    /**
     * Bounded source-only inspection, usable before the enclosing source query finishes. It reserves
     * every declared embedded path independently of current contents and never inspects a target.
     * Unknown parent fields must stay unknown in the caller's projection, not be supplied as absent.
     */
    public static Result inspect(List<Field> fields, Limits limits) {
        Objects.requireNonNull(fields, "fields"); Objects.requireNonNull(limits, "limits");
        if (fields.size() > limits.fields()) throw limit("fields");
        Budget budget = new Budget(limits);
        var names = new ArrayList<Named>(fields.size());
        var distinct = new HashSet<Field>();
        for (Field field : fields) {
            Objects.requireNonNull(field, "field");
            long chars = field.path().size() - 1L;
            for (String segment : field.path()) chars += segment.length();
            budget.bytes(256L + 2L * field.owner().length() + 4L * chars + 40L * field.path().size());
            budget.operations(chars + 1);
            if (!distinct.add(field)) throw ValidationError.invalid("nestFields.repeatedField");
            names.add(new Named(field, String.join(".", field.path())));
        }
        var conflicts = new ArrayList<Conflict>();
        Set<Field> blocked = new LinkedHashSet<>();
        for (int i = 0; i < names.size(); i++) {
            Named left = names.get(i);
            for (int j = i + 1; j < names.size(); j++) {
                Named right = names.get(j);
                budget.operations(1);
                if (!compete(left.field(), right.field())) continue;
                budget.operations(2L * Math.min(left.dotted().length(), right.dotted().length()) + 1);
                if (!overlaps(left.dotted(), right.dotted())) continue;
                if (conflicts.size() == limits.conflicts()) throw limit("conflicts");
                budget.bytes(160);
                Field contributor = contributor(left.field(), right.field());
                Field occupied = contributor == left.field() ? right.field() : left.field();
                conflicts.add(new Conflict(contributor, occupied));
                blocked.add(contributor); blocked.add(occupied);
            }
        }
        var uncontested = new ArrayList<Field>();
        for (Named name : names) if (!blocked.contains(name.field())) uncontested.add(name.field());
        return new Result(uncontested, conflicts);
    }

    private static boolean compete(Field left, Field right) {
        if (left.kind() == FieldKind.PARENT && right.kind() == FieldKind.EMBED_PATH
                || right.kind() == FieldKind.PARENT && left.kind() == FieldKind.EMBED_PATH) return true;
        if (left.kind() != FieldKind.FLAT && right.kind() != FieldKind.FLAT) return false;
        // A single row's literal fields are not separate writers. Its embedded paths are separate claims.
        return left.kind() != right.kind() || !left.owner().equals(right.owner());
    }
    private static Field contributor(Field left, Field right) {
        if (left.kind() == FieldKind.FLAT) return left;
        if (right.kind() == FieldKind.FLAT) return right;
        // Reserve the declared object/array path before examining its current reference or children.
        return left.kind() == FieldKind.EMBED_PATH ? left : right;
    }
    private static boolean overlaps(String left, String right) {
        String shorter = left.length() <= right.length() ? left : right;
        String longer = shorter == left ? right : left;
        return longer.startsWith(shorter) && (longer.length() == shorter.length() || longer.charAt(shorter.length()) == '.');
    }
    private static void requireScope(ValidationObservation actual, ValidationObservation expected) {
        if (!actual.run().equals(expected.run()) || actual.revision() != expected.revision()
                || !actual.root().mappingId().equals(expected.root().mappingId())
                || !actual.root().outputId().equals(expected.root().outputId())
                || !actual.root().rootKey().equals(expected.root().rootKey())) throw ValidationError.invalid("nestFields.observation");
    }
    private static Location location(ValidationObservation parent, Field field) {
        return new Location(parent.root(), new ArrayList<>(field.path()));
    }
    private static void block(Builder root, Location location, Finding cause) {
        root.notCompared(Check.STRUCTURE, location, cause);
        if (root.method() == ValidationMethod.FULL_COLUMN) root.notCompared(Check.VALUES, location, cause);
    }
    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "nestFields." + dimension), null);
    }
    private static final class Budget {
        private final Limits limits;
        private long bytes;
        private long operations;
        private Budget(Limits limits) { this.limits = limits; }
        private void bytes(long amount) {
            if (amount > limits.bytes() - bytes) throw limit("bytes");
            bytes += amount;
        }
        private void operations(long amount) {
            if (amount > limits.operations() - operations) throw limit("operations");
            operations += amount;
        }
    }
}
