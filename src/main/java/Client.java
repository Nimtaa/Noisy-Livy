import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

public class Client {

    private  LivyClient client = null;



    public Client() {
        //problem with uri ...
        client = new LivyClientBuilder().setURI(new URI("")).build();

    }
    public static void main(String[] args) {
        Client s = new Client();
        String thisjar = "/home/nima/IdeaProjects/livytest/out/artifacts/livytest_jar/livytest.jar";
        System.err.printf("Uploading %s to the Spark context...\n", thisjar);
        //client.uploadJar(new File(thisjar)).get();
        try {
            s.client.addJar(new URI(thisjar));
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        System.err.printf("Running PiJob with %d samples...\n",10);
        double pi = 0;
        try {
            pi = s.client.submit(new PiJob(10)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("Pi is roughly: " + pi);
    }
}
