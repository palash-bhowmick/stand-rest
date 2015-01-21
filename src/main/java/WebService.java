import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import com.sun.net.httpserver.HttpServer;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Palash on 1/21/2015.
 */
// The Java class will be hosted at the URI path "/helloworld"

@Path("/table/{tablename}")
public class WebService {
    //todo move to config file
    private static String DB_HOST = "192.168.33.130";
    private static String DB_PORT = "1530";
    private static String DB_INSTANCE = "CRM";

    private static String REST_HOST = "localhost";
    private static String REST_PORT = "8080";

    @GET
    @Produces("application/json")
    public String getResponse(@PathParam("tablename") String tablename) {
        Connection con = null;
        try {
            Class.forName("org.apache.derby.jdbc.ClientDriver");
            con = DriverManager.getConnection("jdbc:derby://" + DB_HOST + ":" + DB_PORT + "/" + DB_INSTANCE + ";ssl=basic");

            Statement statement = con.createStatement();

            ResultSet tables = con.getMetaData().getTables(null, "%", null, new String[]{"TABLE"});
            JSONObject mainObj = new JSONObject();
            JSONArray tableArray = new JSONArray();
            while (tables.next()) {
                int i = 1;
                JSONObject obj = new JSONObject();
                String tableName = tables.getString(3);
                obj.put("tableName", tableName);
                String uri = "http://" + REST_HOST + ":" + REST_PORT + "/" + "table/" + URLEncoder.encode(tableName);
                obj.put("uri", uri);
                tableArray.put(obj);
            }
            mainObj.put("tables", tableArray);
            String formattedTableName = "\"" + DB_INSTANCE + "\".\"" + URLDecoder.decode(tablename) + "\"";
            String sql = "SELECT * FROM " + formattedTableName;
            System.out.println("sql = " + sql);
            ResultSet resultSet = statement.executeQuery(sql);
            int colCount = resultSet.getMetaData().getColumnCount();

            List<String> colNames = new ArrayList<String>();

            for (int i = 1; i <= colCount; i++) {
                colNames.add(resultSet.getMetaData().getColumnName(i));
            }

            JSONArray array = new JSONArray();
            int rowCount = 0;
            while (resultSet.next()) {
                int i = 1;
                JSONObject obj = new JSONObject();
                for (String colName : colNames) {
                    String val = String.valueOf(resultSet.getObject(i++));
                    obj.put(colName, val);
                }
                array.put(obj);
                if (rowCount++ > 100) break; // todo allow pagination
            }
            mainObj.put("tableData", array);
            return mainObj.toString();
        } catch (Exception e) {
            e.printStackTrace();
            JSONArray array = new JSONArray();
            try {
                JSONObject obj = new JSONObject();
                obj.put("status", "error");
                obj.put("msg", e.getMessage());
                array.put(obj);
            } catch (JSONException e1) {
                e1.printStackTrace();
            }
            return array.toString();
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        HttpServer server = HttpServerFactory.create("http://localhost:" + REST_PORT + "/");
        server.start();

        System.out.println("Server running");
        System.out.println("Hit return to stop...");
        System.in.read();
        System.out.println("Stopping server");
        server.stop(0);
        System.out.println("Server stopped");
    }
}
