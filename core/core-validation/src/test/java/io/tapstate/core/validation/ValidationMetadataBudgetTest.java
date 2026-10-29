package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationMetadataBudgetTest {
    @Test void fourMixedLeasesShareOneFixedPoolAndForeignOrReturnedOwnersNeverValidate(){
        var budget=new ValidationMetadataBudget();var owner=new Object();var leases=new ArrayList<ValidationMetadataBudget.Lease>();
        try{
            for(int index=0;index<4;index++)leases.add(budget.acquire(index==0?owner:new Object()));
            assertThat(budget.inUse()).isEqualTo(4);assertThatThrownBy(()->budget.acquire(new Object())).isInstanceOf(TapstateException.class);
            leases.getFirst().requireRetained(owner);assertThatThrownBy(()->leases.getFirst().requireRetained(new Object())).isInstanceOf(TapstateException.class);
            assertThat(budget.inUse()).isEqualTo(4);leases.getFirst().close();assertThatThrownBy(()->leases.getFirst().requireRetained(owner)).isInstanceOf(TapstateException.class);
            try(var next=budget.acquire(new Object())){assertThat(budget.inUse()).isEqualTo(4);}
        }finally{leases.forEach(ValidationMetadataBudget.Lease::close);}
        assertThat(budget.inUse()).isZero();
    }
    @Test void concurrentDoubleCloseReturnsExactlyOneSlot()throws Exception{
        var budget=new ValidationMetadataBudget();var lease=budget.acquire(new Object());
        try(var callers=Executors.newFixedThreadPool(4)){
            var tasks=new ArrayList<Future<?>>();for(int index=0;index<32;index++)tasks.add(callers.submit(lease::close));
            for(var task:tasks)task.get(2,TimeUnit.SECONDS);
        }
        assertThat(budget.inUse()).isZero();var held=new ArrayList<ValidationMetadataBudget.Lease>();
        try{for(int index=0;index<4;index++)held.add(budget.acquire(new Object()));assertThatThrownBy(()->budget.acquire(new Object())).isInstanceOf(TapstateException.class);}
        finally{held.forEach(ValidationMetadataBudget.Lease::close);}
    }
}
