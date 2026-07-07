package io.github.nini4746.cdcview.config;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** "30s", "10m", "1h", "500ms" 형태의 기간 문자열 파서. */
final class Durations {

    private static final Pattern PATTERN = Pattern.compile("^(\\d+)(ms|s|m|h)$");

    private Durations() {
    }

    static Duration parse(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("duration이 null입니다");
        }
        Matcher m = PATTERN.matcher(raw.trim());
        if (!m.matches()) {
            throw new IllegalArgumentException(
                    "잘못된 기간 형식: '" + raw + "' (예: 30s, 10m, 1h, 500ms)");
        }
        long value = Long.parseLong(m.group(1));
        return switch (m.group(2)) {
            case "ms" -> Duration.ofMillis(value);
            case "s" -> Duration.ofSeconds(value);
            case "m" -> Duration.ofMinutes(value);
            case "h" -> Duration.ofHours(value);
            default -> throw new IllegalStateException("unreachable");
        };
    }
}
