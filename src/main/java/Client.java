import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

public class Client {
    private  LivyClient client = null;
    public Client() {
        //problem with uri ...
        try {
           client = new LivyClientBuilder().setURI(new URI("rsc","user:info","localhost",8998,"","",""))
//            client = new LivyClientBuilder().setURI(new URI("http","user:info","localhost",8998,"","",""))
            .build();
//            System.out.println(client);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        Client s = new Client();
        String thisjar = "/home/nima/IdeaProjects/livytest/out/artifacts/livytest_jar/livytest.jar";
        String thisJAR = "/home/nima/IdeaProjects/livytest/target/livytest-1.0-SNAPSHOT.jar";
        System.err.printf("Uploading %s to the Spark context...\n", thisjar);
        //client.uploadJar(new File(thisjar)).get();
        try {
            s.client.addJar(new URI(thisJAR));
        }
        catch (URISyntaxException e) {
            e.printStackTrace();
        }
        System.err.printf("Running PiJob with %d samples...\n",10);
        double pi = 0;
        try {
            pi = s.client.submit(new PiJob(10)).get();
            System.out.println("Pi is roughly: " + pi);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        finally {
//            s.client.stop(true);
          }
        }
    }