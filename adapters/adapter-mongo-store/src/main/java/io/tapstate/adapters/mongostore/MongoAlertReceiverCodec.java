package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.AlertConfiguration.Header;
import io.tapstate.core.alerting.AlertReceiver;
import io.tapstate.core.alerting.AlertReceiver.*;
import java.net.URI;
import java.util.Arrays;
import java.util.Set;
import org.bson.Document;

/** Explicit public receiver fields; this codec never returns the private slot. */
final class MongoAlertReceiverCodec {
    private MongoAlertReceiverCodec() { }
    static Document body(AlertReceiver receiver) {
        Document transport = switch (receiver.transport()) {
            case Email email -> new Document("channelType", "email").append("host", email.host()).append("port", email.port())
                    .append("encryption", email.encryption().name()).append("authentication", email.authentication().name())
                    .append("username", email.username()).append("from", email.from()).append("recipients", email.recipients()).append("trust", email.trust().name());
            case Webhook webhook -> new Document("channelType", "webhook").append("origin", webhook.origin().toASCIIString())
                    .append("endpointDigest", webhook.endpointDigest()).append("authentication", webhook.authentication().name())
                    .append("principal", webhook.principal()).append("trust", webhook.trust().name());
        };
        Document credentials = receiver.credentials() == null ? null : new Document("credentialId", receiver.credentials().credentialId())
                .append("authenticationIdentity", receiver.credentials().authenticationIdentity());
        return new Document("name", receiver.name()).append("enabled", receiver.enabled()).append("bindingRevision", receiver.bindingRevision())
                .append("transport", transport).append("credentials", credentials);
    }
    static AlertReceiver read(Header header, Document body) {
        fields(body, Set.of("name", "enabled", "bindingRevision", "transport", "credentials"));
        require(body.get("enabled") instanceof Boolean && body.get("bindingRevision") instanceof Long);
        Document t = document(body.get("transport"));
        Transport transport = switch (text(t, "channelType")) {
            case "email" -> {
                fields(t, Set.of("channelType", "host", "port", "encryption", "authentication", "username", "from", "recipients", "trust"));
                require(t.get("port") instanceof Integer && t.get("recipients") instanceof java.util.List<?>);
                require(t.getList("recipients", Object.class).size() <= 32 && t.getList("recipients", Object.class).stream().allMatch(String.class::isInstance));
                yield new Email(text(t, "host"), t.getInteger("port"), enumValue(Encryption.values(), text(t, "encryption")),
                        enumValue(Authentication.values(), text(t, "authentication")), nullableText(t, "username"), text(t, "from"),
                        t.getList("recipients", String.class), enumValue(Trust.values(), text(t, "trust")));
            }
            case "webhook" -> {
                fields(t, Set.of("channelType", "origin", "endpointDigest", "authentication", "principal", "trust"));
                yield new Webhook(URI.create(text(t, "origin")), text(t, "endpointDigest"), enumValue(Authentication.values(), text(t, "authentication")),
                        nullableText(t, "principal"), enumValue(Trust.values(), text(t, "trust")));
            }
            default -> throw new IllegalArgumentException("unknown receiver transport");
        };
        CredentialReference reference = null;
        if (body.get("credentials") != null) {
            Document c = document(body.get("credentials")); fields(c, Set.of("credentialId", "authenticationIdentity"));
            reference = new CredentialReference(header.identity(), text(c, "credentialId"), text(c, "authenticationIdentity"));
        }
        return new AlertReceiver(header, text(body, "name"), body.getBoolean("enabled"), body.getLong("bindingRevision"), transport, reference);
    }
    private static String text(Document doc, String name) { require(doc.get(name) instanceof String); return doc.getString(name); }
    private static String nullableText(Document doc, String name) { return doc.get(name) == null ? null : text(doc, name); }
    private static Document document(Object value) { require(value instanceof Document); return (Document) value; }
    private static void fields(Document value, Set<String> expected) { require(value.keySet().equals(expected)); }
    private static void require(boolean valid) { if (!valid) { throw new IllegalArgumentException("invalid receiver encoding"); } }
    private static <E extends Enum<E>> E enumValue(E[] known, String value) {
        return Arrays.stream(known).filter(item -> item.name().equals(value)).findFirst().orElseThrow(IllegalArgumentException::new);
    }
}
