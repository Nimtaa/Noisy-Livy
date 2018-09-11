import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.spark.api.java.function.*;
import org.apache.livy.*;

    public class PiJob implements Job<Double>, Function<Integer, Integer>,
            Function2<Integer, Integer, Integer> {
        private  final int samples;
        public PiJob(int samples) {
            this.samples = samples;
        }
        @Override
        public Double call(JobContext ctx) throws Exception {
            List<Integer> sampleList = new ArrayList<Integer>();
            for (int i = 0; i < samples; i++) {
                sampleList.add(i + 1);
            }
            return 4.0d * ctx.sc().parallelize(sampleList).map(this).reduce(this) / samples;
        }
        @Override
        public Integer call(Integer v1) {
            double x = Math.random();
            double y = Math.random();
            return (x * x + y * y < 1) ? 1 : 0;
        }
        @Override
        public Integer call(Integer v1, Integer v2) {
            return v1 + v2;
        }
        public static void main(String[] args) {
            //String livyUrl = "http://localhost:8998";
            String thisjar = "/home/nima/IdeaProjects/livytest/out/artifacts/livytest_jar/livytest.jar";
            LivyClient client = null;
            try {
                URI livuri = URI.create("http://localhost:8998");
                client = new LivyClientBuilder()
                        .setURI(livuri)
                        .build();
            }  catch (IOException e) {
                e.printStackTrace();
            }
            try {
                System.err.printf("Uploading %s to the Spark context...\n", thisjar);
                client.uploadJar(new File(thisjar)).get();
                System.err.printf("Running PiJob with %d samples...\n",10);
                double pi = client.submit(new PiJob(10)).get();
                System.out.println("Pi is roughly: " + pi);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } finally {
                client.stop(true);
            }
        }
    }




