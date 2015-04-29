package net.ravendb.rachis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;


public class TailFeatherClient implements AutoCloseable {

  protected String[] lastKnownAllVotingNodes;
  protected Future<TailFeatherTopology> topologyTask;
  protected CloseableHttpAsyncClient httpClient;
  protected ObjectMapper objectMapper = new ObjectMapper();
  private ExecutorService executor = Executors.newFixedThreadPool(2);

  public TailFeatherClient(String... nodes) {
    httpClient = HttpAsyncClients.custom().setDefaultRequestConfig(RequestConfig.custom().setRedirectsEnabled(false).build()).build();
    httpClient.start();
    lastKnownAllVotingNodes = nodes;
    topologyTask = findLatestTopology(lastKnownAllVotingNodes);
  }

  @SuppressWarnings("boxing")
  private Future<TailFeatherTopology> findLatestTopology(final String[] nodes) {
    return executor.submit(new Callable<TailFeatherTopology>() {
      @Override
      public TailFeatherTopology call() throws Exception {
        final List<Future<HttpResponse>> tasks = new ArrayList<>(nodes.length);
        for (String node: nodes) {
          HttpGet getTopology = new HttpGet(node + "/tailfeather/admin/flock");
          tasks.add(httpClient.execute(getTopology, null));
        }
        List<TailFeatherTopology> topologies = new ArrayList<>();
        for (Future<HttpResponse> task: tasks) {
          try {
            HttpResponse message = task.get();
            if (message.getStatusLine().getStatusCode() >= 400) {
              continue;
            }
            HttpEntity entity = message.getEntity();
            try {
              topologies.add(objectMapper.readValue(entity.getContent(), TailFeatherTopology.class));
            } finally {
              EntityUtils.consumeQuietly(entity);
            }

          } catch (ExecutionException e) {
            //TODO simply ignore or maybe log
          }
        }

        if (topologies.size() == 0) {
          throw new ClusterNotReachableException();
        }

        long heighestCommitIndex = -1;
        TailFeatherTopology newestTopo = null;
        for (TailFeatherTopology topology: topologies) {
          if (topology.getCommitIndex() > heighestCommitIndex) {
            newestTopo = topology;
            heighestCommitIndex = topology.getCommitIndex();
          }
        }
        if (newestTopo != null) {
          String[] allVoting = newestTopo.findAllVotingNodesUris();
          if (allVoting.length > 0) {
            lastKnownAllVotingNodes = allVoting;
          }
        }
        return newestTopo;
      }
    });
  }

  private HttpResponse contactServer(String relativeUrl) {
    return contactServer(relativeUrl, 3);
  }

  private HttpResponse contactServer(String relativeUrl, int retries) {
    if (retries < 0)
      throw new ClusterNotReachableException();

    try {
      TailFeatherTopology topology = topologyTask.get();
      NodeConnectionInfo leader = topology.findLeader();
      if (leader == null) {
        topologyTask = findLatestTopology(lastKnownAllVotingNodes);
        return contactServer(relativeUrl, retries - 1);
      }

      // now we have a leader, we need to try calling it...
      HttpGet operation = new HttpGet(leader.getUri() + relativeUrl);
      HttpResponse httpResponse = httpClient.execute(operation, null).get();
      if (!Utils.isSuccessfulStatusCode(httpResponse)) {
        // we were sent to a different server, let try that...
        if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_MOVED_TEMPORARILY) {
          Header redirectUri = httpResponse.getHeaders("Location")[0];
          operation = new HttpGet(redirectUri.getValue());
          httpResponse = httpClient.execute(operation, null).get();
          if (Utils.isSuccessfulStatusCode(httpResponse)) {
            // we successfully contacted the redirected server, this is probably the leader, let us ask it for the topology,
            // it will be there for next time we access it
            topologyTask = findLatestTopology(Utils.union(new String[] { redirectUri.getValue() }, topology.findAllVotingNodesUris()));
            return httpResponse;
          }
        }
      }
      return httpResponse;
    } catch (ExecutionException | InterruptedException e) {
      topologyTask = findLatestTopology(lastKnownAllVotingNodes);
      return contactServer(relativeUrl, retries - 1);
    }
  }

  public void set(String key, JsonNode value) throws ClusterNotReachableException {
    contactServer(String.format("/tailfeather/key-val/set?key=%s&val=%s",
      UrlUtils.escapeDataString(key),
      UrlUtils.escapeDataString(value.toString())));
  }

  public JsonNode get(String key) throws IOException, ClusterNotReachableException {
    HttpResponse response = contactServer(String.format("/tailfeather/key-val/read?key=%s", UrlUtils.escapeDataString(key)));
    HttpEntity entity = response.getEntity();
    try {
      JsonNode result = objectMapper.readTree(entity.getContent());
      if (result.get("Missing").asBoolean()) {
        return null;
      }
      return result.get("Value");
    } finally {
      EntityUtils.consumeQuietly(entity);
    }
  }

  public void remove(String key) {
    contactServer(String.format("/tailfeather/key-val/del?key=%s", UrlUtils.escapeDataString(key)));
  }

  @Override
  public void close() throws IOException {
    httpClient.close();
  }


}
