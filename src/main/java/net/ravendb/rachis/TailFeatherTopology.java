package net.ravendb.rachis;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.codehaus.jackson.annotate.JsonProperty;


public class TailFeatherTopology {

  @JsonProperty("State")
  private String state;

  @JsonProperty("CurrentLeader")
  private String currentLeader;

  @JsonProperty("CurrentTerm")
  private Long currentTerm;

  @JsonProperty("CommitIndex")
  private Long commitIndex;

  @JsonProperty("AllVotingNodes")
  private NodeConnectionInfo[] allVotingNodes;

  @JsonProperty("PromotableNodes")
  private NodeConnectionInfo[] promotableNodes;

  @JsonProperty("NonVotingNodes")
  private NodeConnectionInfo[] nonVotingNodes;

  public String getCurrentLeader() {
    return currentLeader;
  }

  public void setCurrentLeader(String currentLeader) {
    this.currentLeader = currentLeader;
  }

  public Long getCurrentTerm() {
    return currentTerm;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public void setCurrentTerm(Long currentTerm) {
    this.currentTerm = currentTerm;
  }

  public Long getCommitIndex() {
    return commitIndex;
  }

  public void setCommitIndex(Long commitIndex) {
    this.commitIndex = commitIndex;
  }

  public NodeConnectionInfo[] getAllVotingNodes() {
    return allVotingNodes;
  }

  public void setAllVotingNodes(NodeConnectionInfo[] allVotingNodes) {
    this.allVotingNodes = allVotingNodes;
  }

  public NodeConnectionInfo[] getPromotableNodes() {
    return promotableNodes;
  }

  public void setPromotableNodes(NodeConnectionInfo[] promotableNodes) {
    this.promotableNodes = promotableNodes;
  }

  public NodeConnectionInfo[] getNonVotingNodes() {
    return nonVotingNodes;
  }

  public void setNonVotingNodes(NodeConnectionInfo[] nonVotingNodes) {
    this.nonVotingNodes = nonVotingNodes;
  }

  public NodeConnectionInfo findLeader() {
    for (NodeConnectionInfo nci: allVotingNodes) {
      if (Objects.equals(currentLeader, nci.getName())) {
        return nci;
      }
    }
    return null;
  }

  public String[] findAllVotingNodesUris() {
    List<String> nodes = new ArrayList<>();
    for (NodeConnectionInfo nci: allVotingNodes) {
      nodes.add(nci.getUri());
    }
    return nodes.toArray(new String[0]);
  }

}
