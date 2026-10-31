package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.runtime.engine.validation.ValidationProcessorRegistry;
import java.util.Objects;
import static io.tapstate.app.ValidationOwnerRouter.*;

/** Member-local command assembly. Original reservations are required; status/stop never reconstruct execution. */
final class NativeValidationCommands implements Commands {
    private final NativeValidationOwnerReservations owners;
    private final NativeValidationRuns runs;
    private final HazelcastInstance member;
    private final ValidationProcessorRegistry processors;
    private final ValidationMemberResources process;
    private final NativeValidationFullJob.Factory factory;
    NativeValidationCommands(NativeValidationOwnerReservations owners,NativeValidationRuns runs,HazelcastInstance member,
            ValidationProcessorRegistry processors,ValidationMemberResources process,NativeValidationFullJob.Factory factory){
        this.owners=Objects.requireNonNull(owners);this.runs=Objects.requireNonNull(runs);this.member=Objects.requireNonNull(member);
        this.processors=Objects.requireNonNull(processors);this.process=Objects.requireNonNull(process);this.factory=Objects.requireNonNull(factory);
    }
    @Override public CommandReply execute(Command command){
        if(!command.request().mode().equals("FULL"))return commandUnavailable(command,Outcome.REJECTED,null);
        var start=runs.original(command.request(),command.owner()).orElse(null);
        if(command.action()==Action.CANCEL_UNUSED){
            var pending=owners.pending(command.request(),command.owner(),true).orElse(null);
            if(pending!=null)return pending(command,pending);
            if(start==null)return commandUnavailable(command,Outcome.REJECTED,null);
        }
        if(command.action()==Action.ACTIVATE&&start==null){
            start=owners.original(command.request(),command.owner());var reservation=owners.take(start);
            runs.start(start,new NativeValidationFullJob(reservation,processors,member,process,runs,factory));
        }
        if(start==null){
            var pending=owners.pending(command.request(),command.owner(),command.action()==Action.STOP).orElse(null);
            if(pending!=null)return pending(command,pending);
            return commandUnavailable(command,Outcome.REJECTED,null);
        }
        if(command.action()==Action.STOP)runs.stop(start.run());
        var status=runs.read(start.run());
        String code=NativeValidationReportFailure.code(status.failure());
        return new CommandReply(Outcome.ACCEPTED,command,status.state().name(),status.admitted(),status.waiting(),status.controlUnknown(),status.retired(),status.drained(),code,runs.tables(start.run()));
    }
    private static CommandReply pending(Command command,NativeValidationOwnerReservations.Pending value){
        return new CommandReply(Outcome.ACCEPTED,command,value.terminal()==null?(value.stopped()?"STOPPED":"STARTING"):value.terminal().name(),
                value.admitted(),false,false,value.terminal()!=null,value.drained(),value.reason());
    }
}
