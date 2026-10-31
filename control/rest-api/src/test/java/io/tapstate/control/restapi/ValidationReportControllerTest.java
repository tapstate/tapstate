package io.tapstate.control.restapi;

import io.tapstate.control.core.ValidationReportService;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.mock.web.MockHttpServletResponse;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.*;

class ValidationReportControllerTest {
    @Test void slowSerializationRetainsTheOriginalFrameUntilTheWriterActuallyFinishes()throws Exception{
        var budget=new ValidationMetadataBudget();var controller=controller(budget);var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        var output=new ServletOutputStream(){
            @Override public boolean isReady(){return true;}
            @Override public void setWriteListener(WriteListener listener){throw new UnsupportedOperationException();}
            @Override public void write(int value)throws IOException{entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("serialization barrier timeout");}
                catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException(interrupted);}}
        };
        var response=new MockHttpServletResponse(){@Override public ServletOutputStream getOutputStream(){return output;}};
        try(var writer=Executors.newSingleThreadExecutor()){
            var writing=writer.submit(()->{try{controller.history("flow","task","0","10",response);}catch(IOException failed){throw new UncheckedIOException(failed);}});
            try{assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();assertThat(budget.inUse()).isOne();release.countDown();writing.get(2,TimeUnit.SECONDS);}
            finally{release.countDown();}
        }
        assertThat(budget.inUse()).isZero();assertThat(response.getHeader("Cache-Control")).isEqualTo("no-store");
    }
    @Test void aDisconnectedWriterReturnsItsFrameAndPreservesTheTransportFailure(){
        var budget=new ValidationMetadataBudget();var controller=controller(budget);var disconnect=new IOException("fixture client disconnected");
        var response=new MockHttpServletResponse(){@Override public ServletOutputStream getOutputStream(){return new ServletOutputStream(){
            @Override public boolean isReady(){return true;}
            @Override public void setWriteListener(WriteListener listener){throw new UnsupportedOperationException();}
            @Override public void write(int value)throws IOException{throw disconnect;}
        };}};
        assertThatThrownBy(()->controller.history("flow","task","0","10",response)).hasRootCause(disconnect);
        assertThat(budget.inUse()).isZero();
    }
    @Test void cancellationIsNotAReadExitReceiptAndTheControllerClosesOnlyAfterActualReturn()throws Exception{
        var budget=new ValidationMetadataBudget();var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var finished=new CountDownLatch(1);
        var controller=controller(budget,()->{entered.countDown();while(release.getCount()!=0)try{release.await();}catch(InterruptedException ignored){/* Original I/O may ignore cancellation. */}});
        try(var caller=Executors.newSingleThreadExecutor()){
            var work=caller.submit(()->{try{controller.history("flow","task","0","10",new MockHttpServletResponse());}catch(IOException failed){throw new UncheckedIOException(failed);}finally{finished.countDown();}});
            try{
                assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();work.cancel(true);assertThat(work.isCancelled()).isTrue();
                assertThat(finished.getCount()).isOne();assertThat(budget.inUse()).isOne();release.countDown();assertThat(finished.await(2,TimeUnit.SECONDS)).isTrue();
            }finally{release.countDown();}
        }
        assertThat(budget.inUse()).isZero();
    }
    private static ValidationReportController controller(ValidationMetadataBudget budget){return controller(budget,()->{});}
    private static ValidationReportController controller(ValidationMetadataBudget budget,Runnable beforeRead){
        var reports=new ValidationReports(){
            @Override public Page<Summary> history(String pipeline,String task,ValidationMode mode,Paging page){beforeRead.run();return new Page<>(List.of(),0,page);}
            @Override public Optional<Snapshot> find(Reference reference){throw new AssertionError("unused");}
            @Override public Page<RootSummary> roots(Reference reference,Paging page){throw new AssertionError("unused");}
            @Override public Optional<RootDetail> root(Reference reference,String hash){throw new AssertionError("unused");}
            @Override public Mirror mirror(TerminalMirror command){throw new AssertionError("read cannot publish");}
            @Override public Mirror observe(Observation command){throw new AssertionError("read cannot publish");}
            @Override public Cleanup retain(Retention command){throw new AssertionError("read cannot expire");}
        };
        var beans=new DefaultListableBeanFactory();beans.registerSingleton("reports",new ValidationReportService(reports,budget));
        return new ValidationReportController(beans.getBeanProvider(ValidationReportService.class),JsonMapper.builder().build());
    }
}
