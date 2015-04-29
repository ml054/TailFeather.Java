package net.ravendb.rachis;

import java.util.HashSet;
import java.util.Set;

import org.apache.http.HttpResponse;


public class Utils {
  public static String[] union(String[] first, String[] second) {
    Set<String> set = new HashSet<>();
    for (String s: first) {
      set.add(s);
    }
    for (String s: second) {
      set.add(s);
    }
    return set.toArray(new String[0]);
  }

  public static boolean isSuccessfulStatusCode(HttpResponse response) {
    int statusCode = response.getStatusLine().getStatusCode();
    return statusCode >= 200 && statusCode <= 299;
  }
}
