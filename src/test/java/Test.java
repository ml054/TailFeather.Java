import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.TextNode;

import net.ravendb.rachis.TailFeatherClient;



public class Test {
  public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
    TailFeatherClient client = new TailFeatherClient("http://localhost:9077/");


    for (int i = 0; i< 10000; i++) {
      try {
        System.out.println("pass #" + i);
        client.set("a", TextNode.valueOf("x"));
        JsonNode jsonNode = client.get("a");

      } catch (Exception e) {
        e.printStackTrace();
      }

      Thread.sleep(250);
      System.in.read();
    }
  }
}
