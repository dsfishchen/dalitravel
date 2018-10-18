package com.dalitravel.bigdataservice.repository.common;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.core.env.Environment;

import java.sql.*;
import java.util.*;

@Service
public class VerticaDao {

    @Autowired
    private Environment env;

    public List<Map> execSql(String sql){

        List ls = new ArrayList<Map>();
/*
        String url = env.getProperty("vertica.url");
        String user = env.getProperty("vertica.user");
        String password = env.getProperty("vertica.password");
        String loginTimeout = env.getProperty("vertica.loginTimeout");
        String binaryBatchInsert = env.getProperty("vertica.binaryBatchInsert");

        Properties myProp = new Properties();
        myProp.put("user", user);
        myProp.put("password", password);
        myProp.put("loginTimeout", loginTimeout);
        myProp.put("binaryBatchInsert", binaryBatchInsert);

        Connection connection = null;
        Statement statement = null;
        ResultSet results = null;
        try {
            Class.forName("com.vertica.jdbc.Driver");
            connection = DriverManager.getConnection(url, myProp);
            statement = connection.createStatement();
            // Set AutoCommit to false to allow Vertica to reuse the same
            // COPY statement
            //connection.setAutoCommit(false);
            results = statement.executeQuery(sql);

            ResultSetMetaData rsmd = results.getMetaData();
            while (results.next()) {
                Map map = new HashMap();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    map.put(rsmd.getColumnName(i), results.getString(rsmd.getColumnName(i)));
                }
                ls.add(map);
            }

        } catch (SQLTransientConnectionException connException) {
            // There was a potentially temporary network error
            // Could automatically retry a number of times here, but
            // instead just report error and exit.
            System.out.print("Network connection issue: ");
            System.out.print(connException.getMessage());
            System.out.println(" Try again later!");
        } catch (SQLInvalidAuthorizationSpecException authException) {
            // Either the username or password was wrong
            System.out.print("Could not log into database: ");
            System.out.print(authException.getMessage());
            System.out.println(" Check the login credentials and try again.");
        } catch (Exception e) {
            // Catch-all for other exceptions
            e.printStackTrace();
        }finally {//关闭连接
            try {
                if (results != null) {
                    results.close();
                }
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
*/
        return ls;
    }
}
