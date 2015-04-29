package net.ravendb.rachis;


public class ClusterNotReachableException extends RuntimeException {

  public ClusterNotReachableException() {
    super();
  }

  public ClusterNotReachableException(String message, Throwable cause) {
    super(message, cause);
  }

  public ClusterNotReachableException(String message) {
    super(message);
  }

  public ClusterNotReachableException(Throwable cause) {
    super(cause);
  }

}
