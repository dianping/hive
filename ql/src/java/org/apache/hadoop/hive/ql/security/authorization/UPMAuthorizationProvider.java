package org.apache.hadoop.hive.ql.security.authorization;

import com.google.common.collect.Lists;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonRootName;

import java.io.IOException;
import java.util.List;

import static org.codehaus.jackson.map.DeserializationConfig.Feature.*;

/**
 * Created by sunyerui on 16/11/17.
 */
public class UPMAuthorizationProvider extends HiveAuthorizationProviderBase {

  String upmUri;
  String appKey;
  int upmServiceTimeout = 30000;

  @Override
  public void init(Configuration conf) throws HiveException {
    upmUri = conf.get("hive.dp.authorization.upm.uri");
    appKey = conf.get("hive.dp.authorization.upm.appkey");
    upmServiceTimeout = conf.getInt("hive.dp.authorization.upm.timeout", 30000);
    LOG.info("UPMAuthorizationProvider init, uri" + upmUri + " appKey " + appKey);
  }

  @Override
  public void authorize(Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {
    LOG.debug(String.format("UPM authorize user %s, just skip", authenticator.getUserName()));
  }

  @Override
  public void authorize(Database db, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {
    LOG.debug(String.format("UPM authorize user %s database %s, just skip", authenticator.getUserName(), db.getName()));
  }

  @Override
  public void authorize(Table table, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {
    LOG.debug(String.format("UPM authorize user %s database %s table %s without columns", authenticator.getUserName(), table.getDbName(), table.getTableName()));
    authorize(table, null, null, readRequiredPriv, writeRequiredPriv);
  }

  @Override
  public void authorize(Partition part, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {
    LOG.debug(String.format("UPM authorize user %s table %s partition %s, just skip", authenticator.getUserName(), part.getTable(), part.getName()));
  }

  @Override
  public void authorize(Table table, Partition part, List<String> columns, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {
    LOG.debug(String.format("UPM authorize user %s database %s table %s with columns", authenticator.getUserName(), table.getDbName(), table.getTableName()));
    // Only check authorization of inputs for read privilege
    for (Privilege privilege : readRequiredPriv) {
      // Found privileges need to authorize
      if (privilege.getPriv() == PrivilegeType.ALL || privilege.getPriv() == PrivilegeType.SELECT) {
        upmAuthorize(table.getDbName(), table.getTableName(), columns);
        break;
      }
    }
  }

  private static class UPMAuthorizeRequest {
    static class UPMAuthorizeRequestTable {
      final String db;
      final String table;
      final List<String> columns;

      UPMAuthorizeRequestTable(String db, String table, List<String> columns) {
        this.db = db;
        this.table = table;
        this.columns = columns;
      }

      @JsonProperty("db")
      public String getDb() {
        return db;
      }

      @JsonProperty("table")
      public String getTable() {
        return table;
      }

      @JsonProperty("columns")
      public List<String> getColumns() {
        return columns;
      }
    }

    final String app = "hive";
    final String appKey;
    final String user;
    final List<UPMAuthorizeRequestTable> tables;

    UPMAuthorizeRequest(String appKey, String user, String db, String table, List<String> columns) {
      this.appKey = appKey;
      this.user = user;
      this.tables = Lists.newArrayList(new UPMAuthorizeRequestTable(db, table, columns));
    }

    @JsonProperty("app")
    public String getApp() {
      return app;
    }

    @JsonProperty("appkey")
    public String getAppKey() {
      return appKey;
    }

    @JsonProperty("auth")
    public String getUser() {
      return user;
    }

    @JsonProperty("tables")
    public List<UPMAuthorizeRequestTable> getTables() {
      return tables;
    }
  }

  @JsonRootName("data")
  private static class UPMResponse {
    public boolean success;
    public List<String> tables;
    public List<String> columns;
  }

  private void upmAuthorize(String db, String table, List<String> columns) throws AuthorizationException {
    boolean authorizeSuccess = true;
    // If columns is not specified, fill with '*' meaning all columns
    if (columns == null || columns.isEmpty()) {
      columns = Lists.newArrayList("*");
    }
    String user = getAuthenticator().getUserName();
    UPMAuthorizeRequest request = new UPMAuthorizeRequest(appKey, user, db, table, columns);
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(UNWRAP_ROOT_VALUE, true);
    String responseString = "";
    try {
      responseString = postUPMService(mapper.writeValueAsString(request));
      UPMResponse response = mapper.readValue(responseString, UPMResponse.class);
      authorizeSuccess = response.success;
    } catch (Exception e) {
      // Any exceptions meaning authorize failed, except upm post request related exceptions
      LOG.warn("UPMAuthorize failed, response " + responseString + ", exception " + e, e);
      authorizeSuccess = false;
    }

    if (!authorizeSuccess) {
      throw new AuthorizationException(String.format("User %s authorize read table %s.%s columns %s failed",
              user, db, table, StringUtils.join(",", columns)));
    }
  }

  private String postUPMService(String requestString) {
    HttpClient client = new HttpClient();
    client.getHttpConnectionManager().getParams().setConnectionTimeout(upmServiceTimeout);
    client.getHttpConnectionManager().getParams().setSoTimeout(upmServiceTimeout);
    PostMethod method = new PostMethod(upmUri);
    method.getParams().setParameter(HttpMethodParams.HTTP_CONTENT_CHARSET, "utf-8");
    method.addRequestHeader("Content-Type", "application/json");
    method.setRequestEntity(new StringRequestEntity(requestString));

    // If post request failed, we assume authorize success, here'e the default response string meaing authorize success
    String responseString = "{\"data\": {\"success\": true, \"tables\": [], \"columns\": []}}";
    try {
      int statusCode = client.executeMethod(method);
      responseString = method.getResponseBodyAsString();
      LOG.debug("UPMService post response " + responseString);
    } catch (IOException e) {
      LOG.warn("UPMService post exception " + e, e);
    } finally {
      method.releaseConnection();
    }

    return responseString;
  }

  // Test for json serialization and deserialization
  public static void main(String[] args) throws IOException {
    System.out.println("Hello World");
    UPMAuthorizeRequest request = new UPMAuthorizeRequest("a", "b", "c", "d", Lists.newArrayList("e"));
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(UNWRAP_ROOT_VALUE, true);
    System.out.println("Request: " + mapper.writeValueAsString(request));
    String responseString = "{\"data\": {\"success\": true, \"tables\": [], \"columns\": []}}";
    UPMResponse response = mapper.readValue(responseString, UPMResponse.class);
    System.out.println("Response: " + response + ", success " + response.success);
  }
}

