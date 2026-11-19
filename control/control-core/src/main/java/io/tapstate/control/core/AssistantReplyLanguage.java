package io.tapstate.control.core;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Recognizes clear English and French prose for the proposal retry. Ambiguous text follows provider guidance. */
enum AssistantReplyLanguage {
    ENGLISH, FRENCH;

    private static final Pattern REPLY_IN = Pattern.compile(
            "(?iu)\\b(?:reply|answer|respond|explain|write)\\b[^.!?\\r\\n]{0,40}?\\bin\\s+([\\p{L}]+)"
                    + "|\\b(?:r\\u00e9ponds|r\\u00e9pondez|explique|expliquez)\\b[^.!?\\r\\n]{0,40}?\\ben\\s+([\\p{L}]+)");
    private static final Pattern WORD = Pattern.compile("`[^`]*(?:`|$)|[\\p{L}\\p{N}_]+");
    private static final Set<String> ENGLISH_WORDS = Set.of("i", "would", "you", "your", "we", "the", "this",
            "an", "and", "will", "from", "into", "with", "can", "please");
    private static final Set<String> FRENCH_WORDS = Set.of("je", "vous", "nous", "le", "les", "une", "des",
            "du", "dans", "pour", "avec", "cette", "votre", "sont", "sur");

    static AssistantReplyLanguage requested(List<Map<String, String>> messages) {
        for (var turn : messages.reversed()) {
            if (!"user".equals(turn.get("role"))) continue;
            String text = turn.get("content");
            if (text == null) return null;
            var override = REPLY_IN.matcher(text);
            if (override.find()) {
                String name = override.group(1) == null ? override.group(2) : override.group(1);
                return switch (name.toLowerCase(Locale.ROOT)) {
                    case "english", "anglais" -> ENGLISH;
                    case "french", "fran\u00e7ais" -> FRENCH;
                    default -> null;
                };
            }
            return detect(text, true);
        }
        return null;
    }

    static AssistantReplyLanguage detect(CharSequence text, boolean complete) {
        var words = WORD.matcher(text);
        Set<String> english = new HashSet<>();
        Set<String> french = new HashSet<>();
        while (words.find()) {
            if (!complete && words.end() == text.length()) break;
            String word = words.group().toLowerCase(Locale.ROOT);
            if (ENGLISH_WORDS.contains(word)) english.add(word);
            if (FRENCH_WORDS.contains(word)) french.add(word);
        }
        if (english.size() >= 2 && french.isEmpty()) return ENGLISH;
        if (french.size() >= 2 && english.isEmpty()) return FRENCH;
        return null;
    }
}
