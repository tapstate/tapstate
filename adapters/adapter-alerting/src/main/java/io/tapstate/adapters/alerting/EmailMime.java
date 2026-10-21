package io.tapstate.adapters.alerting;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Locale;

/** Freezes the complete SMTP DATA message once, before an intent is accepted. */
public final class EmailMime {
    public static final int MESSAGE_LIMIT = 256 * 1024;
    private EmailMime() { }
    public record Result(byte[] bytes, NotificationRenderer.Failure failure) {
        public Result { bytes = bytes == null ? null : bytes.clone(); }
        @Override public byte[] bytes() { return bytes == null ? null : bytes.clone(); }
        public boolean successful() { return bytes != null; }
    }
    public static Result freeze(String deliveryId, Instant createdAt, String from, String to, NotificationRenderer.Snapshot rendered) {
        if (deliveryId == null || !deliveryId.matches("[A-Za-z0-9._-]{1,256}") || createdAt == null
                || !mailbox(from) || !mailbox(to) || rendered == null
                || rendered.subject().codePoints().anyMatch(Character::isISOControl)) {
            return new Result(null, NotificationRenderer.Failure.INVALID_FACTS);
        }
        String hash = digest(deliveryId);
        String boundary = "tapstate_" + hash.substring(0, 32);
        String date = DateTimeFormatter.ofPattern("EEE, dd MMM uuuu HH:mm:ss xx", Locale.US).withZone(ZoneOffset.UTC).format(createdAt);
        String message = "Date: " + date + "\r\nFrom: <" + from + ">\r\nTo: <" + to + ">\r\n"
                + "Message-ID: <" + hash + "@tapstate.invalid>\r\nSubject: " + encodedSubject(rendered.subject()) + "\r\n"
                + "MIME-Version: 1.0\r\nContent-Type: multipart/alternative; boundary=\"" + boundary + "\"\r\n\r\n"
                + part(boundary, "text/plain", rendered.text()) + part(boundary, "text/html", rendered.html())
                + "--" + boundary + "--\r\n";
        byte[] bytes = message.getBytes(StandardCharsets.US_ASCII);
        return bytes.length > MESSAGE_LIMIT ? new Result(null, NotificationRenderer.Failure.PAYLOAD_BUDGET) : new Result(bytes, null);
    }
    /** Supported addr-spec subset: ASCII unquoted local parts and DNS hostnames, no header display syntax. */
    public static boolean mailbox(String value) {
        if (value == null || value.length() > 254 || value.indexOf('@') < 1 || value.indexOf('@') != value.lastIndexOf('@')) { return false; }
        String local = value.substring(0, value.indexOf('@'));
        String domain = value.substring(value.indexOf('@') + 1);
        if (local.length() > 64 || !local.matches("[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+(\\.[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+)*") || domain.length() > 253) { return false; }
        String[] labels = domain.split("\\.", -1);
        if (labels.length < 2) { return false; }
        for (String label : labels) {
            if (!label.matches("[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?")) { return false; }
        }
        return true;
    }
    private static String part(String boundary, String type, String content) {
        return "--" + boundary + "\r\nContent-Type: " + type + "; charset=UTF-8\r\nContent-Transfer-Encoding: base64\r\n\r\n"
                + Base64.getMimeEncoder(76, new byte[]{13, 10}).encodeToString(content.getBytes(StandardCharsets.UTF_8)) + "\r\n";
    }
    private static String encodedSubject(String subject) {
        StringBuilder result = new StringBuilder();
        StringBuilder chunk = new StringBuilder();
        int bytes = 0;
        for (int i = 0; i < subject.length();) {
            int c = subject.codePointAt(i);
            String point = new String(Character.toChars(c));
            int size = point.getBytes(StandardCharsets.UTF_8).length;
            if (bytes + size > 42) {
                if (!result.isEmpty()) { result.append("\r\n "); }
                result.append("=?UTF-8?B?").append(Base64.getEncoder().encodeToString(chunk.toString().getBytes(StandardCharsets.UTF_8))).append("?=");
                chunk.setLength(0); bytes = 0;
            }
            chunk.append(point); bytes += size; i += Character.charCount(c);
        }
        if (!chunk.isEmpty()) {
            if (!result.isEmpty()) { result.append("\r\n "); }
            result.append("=?UTF-8?B?").append(Base64.getEncoder().encodeToString(chunk.toString().getBytes(StandardCharsets.UTF_8))).append("?=");
        }
        return result.toString();
    }
    private static String digest(String id) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(id.getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException ex) { throw new IllegalStateException("required digest is unavailable", ex); }
    }
}
