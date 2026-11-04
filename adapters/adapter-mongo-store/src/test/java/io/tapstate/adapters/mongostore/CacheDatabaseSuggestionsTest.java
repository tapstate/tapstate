package io.tapstate.adapters.mongostore;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import com.mongodb.MongoException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class CacheDatabaseSuggestionsTest {
    @Test void skipsOccupiedCandidatesAndReturnsTheFirstVerifiedEmptyName() {
        var suffixes = new ArrayDeque<>(List.of(483921, 483922, 483923));
        var checked = new ArrayList<String>();

        var suggestion = CacheDatabaseSuggestions.find("tapstate_cache", suffixes::removeFirst, name -> {
            checked.add(name);
            return checked.size() < 3;
        });

        assertThat(suggestion).contains("tapstate_cache_483923");
        assertThat(checked).containsExactly("tapstate_cache_483921", "tapstate_cache_483922", "tapstate_cache_483923");
    }

    @Test void stopsAfterFiveOccupiedCandidatesWithoutReturningAnUncheckedName() {
        var generated = new AtomicInteger();
        var checked = new ArrayList<String>();

        var suggestion = CacheDatabaseSuggestions.find("tapstate_cache",
                () -> 483921 + generated.getAndIncrement(), name -> {
                    checked.add(name);
                    return true;
                });

        assertThat(suggestion).isEmpty();
        assertThat(generated).hasValue(5);
        assertThat(checked).hasSize(5);
    }

    @Test void skipsOutOfRangeAndRepeatedSuffixesBeforeCheckingTheNextCandidate() {
        var suffixes = new ArrayDeque<>(List.of(42, 483921, 483921, 483922));
        var checked = new ArrayList<String>();

        var suggestion = CacheDatabaseSuggestions.find("tapstate_cache", suffixes::removeFirst, name -> {
            checked.add(name);
            return checked.size() == 1;
        });

        assertThat(suggestion).contains("tapstate_cache_483922");
        assertThat(checked).containsExactly("tapstate_cache_483921", "tapstate_cache_483922");
    }

    @Test void returnsNoSuggestionWhenCheckingACandidateFails() {
        var checked = new AtomicInteger();

        var suggestion = CacheDatabaseSuggestions.find("tapstate_cache", () -> 483921, name -> {
            checked.incrementAndGet();
            throw new MongoException("metadata unavailable");
        });

        assertThat(suggestion).isEmpty();
        assertThat(checked).hasValue(1);
    }

    @Test void keepsSuggestedNamesWithinMongoDatabaseLimits() {
        var suggestion = CacheDatabaseSuggestions.find("x".repeat(70), () -> 483921, name -> false);

        assertThat(suggestion).isPresent();
        String name = suggestion.orElseThrow();
        assertThat(name).matches("x+_483921");
        assertThat(MongoDatabaseNames.isValid(name)).isTrue();
    }
}
