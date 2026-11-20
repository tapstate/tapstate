package io.tapstate.app;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class BootstrapWorkerParallelismTest {
    @Test void aColdLowCoreProcessUsesPooledCallbacksAndHonorsAnExplicitOverride()throws Exception{
        assertThat(parallelism(1,null)).isEqualTo("2");
        assertThat(parallelism(2,null)).isEqualTo("2");
        assertThat(parallelism(4,null)).isEqualTo("3");
        assertThat(parallelism(2,"1")).isEqualTo("1");
    }
    private static String parallelism(int cores,String explicit)throws Exception{
        var args=new ArrayList<String>();args.add(Path.of(System.getProperty("java.home"),"bin","java").toString());
        args.add("-XX:ActiveProcessorCount="+cores);if(explicit!=null)args.add("-Djava.util.concurrent.ForkJoinPool.common.parallelism="+explicit);
        args.add("-cp");args.add(System.getProperty("java.class.path"));args.add(Witness.class.getName());
        var process=new ProcessBuilder(args).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        assertThat(process.waitFor(10,TimeUnit.SECONDS)).isTrue();assertThat(process.exitValue()).isZero();
        return new String(process.getInputStream().readAllBytes(),java.nio.charset.StandardCharsets.UTF_8).trim();
    }
    public static class Witness {
        public static void main(String[] args){Bootstrap.configureWorkerParallelism();System.out.println(java.util.concurrent.ForkJoinPool.getCommonPoolParallelism());}
    }
}
