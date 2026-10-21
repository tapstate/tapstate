package io.tapstate.core.alerting;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.Objects;

/** One explicit scoped silence, optionally repeating at a fixed UTC interval. It never changes an episode. */
public record AlertSilenceWindow(AlarmKey.Scope scope, boolean allResources, Set<AlarmKey.Resource> resources,
        Set<String> typeIds, Instant startsAt, Duration duration, Duration period, Instant endsAt) {
    public AlertSilenceWindow {
        Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(startsAt, "startsAt");
        AlertValidation.require(resources != null && resources.size() <= 100 && resources.stream().allMatch(Objects::nonNull), "silenceResources", "invalid-resources");
        AlertValidation.require(allResources ? resources.isEmpty() : !resources.isEmpty(), "silenceResources", "explicit-scope-or-resources-required");
        AlertValidation.require(typeIds != null && typeIds.size() <= 16 && typeIds.stream().allMatch(Objects::nonNull), "silenceTypes", "invalid-types");
        typeIds.forEach(id -> {
            AlertValidation.text(id, "silenceType", 128);
            AlertValidation.require(id.matches("[a-z][a-z0-9]*([.-][a-z0-9]+)*"), "silenceType", "invalid-id");
        });
        resources = Set.copyOf(resources); typeIds = Set.copyOf(typeIds);
        duration = AlertValidation.duration(duration, "silenceDuration", false, Duration.ofDays(7));
        AlertValidation.require(duration.getNano() == 0, "silenceDuration", "whole-seconds-required");
        if (period != null) {
            period = AlertValidation.duration(period, "silencePeriod", false, Duration.ofDays(365));
            AlertValidation.require(period.getNano() == 0 && period.compareTo(duration) >= 0, "silencePeriod", "must-cover-window");
        }
        AlertValidation.require(endsAt == null || endsAt.isAfter(startsAt), "silenceEnd", "must-follow-start");
    }
    public boolean matches(AlarmKey key, Instant now) {
        if (!scope.equals(key.scope()) || !allResources && !resources.contains(key.resource())
                || !typeIds.isEmpty() && !typeIds.contains(key.typeId()) || now.isBefore(startsAt)
                || endsAt != null && !now.isBefore(endsAt)) { return false; }
        Duration since = Duration.between(startsAt, now);
        Duration offset = period == null ? since : since.minus(period.multipliedBy(since.dividedBy(period)));
        return offset.compareTo(duration) < 0;
    }
}
