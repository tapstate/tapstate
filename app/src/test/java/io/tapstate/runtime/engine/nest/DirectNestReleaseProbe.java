package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.test.*;
import io.tapstate.core.event.*;
import io.tapstate.core.model.*;
import io.tapstate.runtime.engine.ReplayFloor;
import java.io.*;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

/** A serialized pending handoff survives a discarded processor and its unsent output. */
public final class DirectNestReleaseProbe {
    private DirectNestReleaseProbe(){}
    public static Pending seed(ReplayFloor floor) throws Exception {
        var child=new Embed("child",Map.of("parent_id","parent_id"),EmbedAs.ARRAY,"children",List.of("child_id"),null,null,null);
        var parent=new Embed("parent",Map.of("root_id","root_id"),EmbedAs.ARRAY,"parents",List.of("parent_id"),null,null,List.of(child));
        var tree=new TransformBody.Nest(null,null,new NestRoot("root",List.of("root_id"),null,null,List.of(parent)));
        var tables=Map.of("root",new NestTable("roots",List.of("root_id")),"parent",new NestTable("orders",List.of("parent_id")),"child",new NestTable("quiet",List.of("child_id")));
        var vertex=NestTopology.compile("flow","doc",tree,tables::get).vertexAt(List.of("parents"));
        var store=new Bytes();var out=new TestOutbox(1);var processor=new ResolverProcessor(vertex,store,(from,row) -> {},floor);
        processor.init(out,new TestProcessorContext());var fields=Map.<String,Object>of("parent_id","P1","root_id","R1");
        feed(processor,0,Envelope.delete(1,"orders",fields,null).withOrder(new SourceOrder(1,1)));out.drainQueueAndReset(0,new ArrayList<>(),false);
        feed(processor,1,Envelope.insert(3,"quiet",Map.of("child_id","C1","parent_id","P1","payload","waiting"),null).withOrder(new SourceOrder(1,3)));
        feed(processor,0,Envelope.insert(2,"orders",fields,null).withOrder(new SourceOrder(1,2)));
        var sent=new ArrayList<Object>();out.drainQueueAndReset(0,sent,false);assertThat(sent).hasSize(1);
        assertThat(store.load(List.of("P1")).releases()).hasSize(1);return new Pending(vertex,store);
    }
    public static final class Pending {
        private final NestVertex vertex;private final Bytes store;
        private Pending(NestVertex vertex,Bytes store){this.vertex=vertex;this.store=store;}
        public void assertRecovered(ReplayFloor original) throws Exception {
            var out=new TestOutbox(16);var restarted=new ResolverProcessor(vertex,store,(from,row) -> {},original);restarted.init(out,new TestProcessorContext());
            feed(restarted,vertex.inbound().size(),new NestReleaseRecovery.Wake(List.of("P1")));
            var delivered=new ArrayList<Object>();out.drainQueueAndReset(0,delivered,false);
            assertThat(delivered).anySatisfy(item -> assertThat(((KeyedElement)item).element().fields()).containsEntry("payload","waiting"));
            assertThat(store.load(List.of("P1")).releases()).hasSize(1);
        }
    }
    private static void feed(ResolverProcessor processor,int ordinal,Object item){var inbox=new TestInbox();inbox.queue().add(item);processor.process(ordinal,inbox);}
    private static final class Bytes implements NestStore<ResolverState> {
        private final Map<Object,byte[]> values=new HashMap<>();
        public ResolverState load(Object key){var bytes=values.get(key);if(bytes==null)return null;try(var input=new ObjectInputStream(new ByteArrayInputStream(bytes))){return (ResolverState)input.readObject();}catch(IOException|ClassNotFoundException failure){throw new AssertionError(failure);}}
        public void save(Object key,ResolverState state){try{var bytes=new ByteArrayOutputStream();try(var output=new ObjectOutputStream(bytes)){output.writeObject(state);}values.put(key,bytes.toByteArray());}catch(IOException failure){throw new AssertionError(failure);}}
        public void remove(Object key){values.remove(key);}public long count(){return values.size();}
    }
}
