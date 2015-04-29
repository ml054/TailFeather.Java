package net.ravendb.rachis;

import org.codehaus.jackson.annotate.JsonProperty;


public class NodeConnectionInfo {
  @JsonProperty("Uri")
  private String uri;

  @JsonProperty("Name")
  private String name;

  @JsonProperty("Username")
  private String username;

  @JsonProperty("Password")
  private String password;

  @JsonProperty("Domain")
  private String domain;

  @JsonProperty("ApiKey")
  private String apiKey;

  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getDomain() {
    return domain;
  }

  public void setDomain(String domain) {
    this.domain = domain;
  }

  public String getApiKey() {
    return apiKey;
  }

  public void setApiKey(String apiKey) {
    this.apiKey = apiKey;
  }

}
