package io.tapstate.adapters.mongostore;

import com.mongodb.event.*;
import java.util.function.Consumer;

/** Optional same-thread diagnostics. No command body or credentials are retained. */
final class MongoCommandObservation {
    static final ThreadLocal<Consumer<String>> CURRENT=new ThreadLocal<>();
    static final ThreadLocal<java.util.function.BiConsumer<String,String>> COLLECTIONS=new ThreadLocal<>();
    static final ThreadLocal<java.util.function.LongConsumer> BYTES=new ThreadLocal<>();
    private static void size(org.bson.BsonDocument value){var observer=BYTES.get();if(observer!=null)observer.accept(new org.bson.RawBsonDocument(value,new org.bson.codecs.BsonDocumentCodec()).getByteBuffer().remaining());}
    static final CommandListener LISTENER=new CommandListener(){
        @Override public void commandStarted(CommandStartedEvent event){var observer=CURRENT.get();if(observer!=null)observer.accept(event.getCommandName());size(event.getCommand());var collections=COLLECTIONS.get();if(collections!=null){var target=event.getCommand().get(event.getCommandName());collections.accept(event.getCommandName(),target!=null&&target.isString()?target.asString().getValue():"");}}
        @Override public void commandSucceeded(CommandSucceededEvent event){size(event.getResponse());}
    };
    private MongoCommandObservation() { }
}
