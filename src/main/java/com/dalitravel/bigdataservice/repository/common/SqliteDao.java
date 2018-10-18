package com.dalitravel.bigdataservice.repository.common;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class SqliteDao {
    @Autowired
    private Environment env;
/*
    public List<Map> execSql(String sql){

        List<Map> list = new ArrayList<Map>();

        String slhost = env.getProperty("spring.datasource.sqlite.url");
        String sluname = env.getProperty("spring.datasource.sqlite.username");
        String slpwd = env.getProperty("spring.datasource.sqlite.password");

        Connection connection = null;
        Statement statement = null;
        ResultSet results = null;
        try{
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection(slhost);
            statement = connection.createStatement();
            statement.setQueryTimeout(30); // set timeout to 30 sec.
            // 执行查询语句
            results = statement.executeQuery(sql);
            ResultSetMetaData rsmd = results.getMetaData();
            while (results.next()) {
                Map map = new HashMap();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    map.put(rsmd.getColumnName(i), results.getString(rsmd.getColumnName(i)));
                }
                list.add(map);
            }
        }catch (Exception e) {
            e.printStackTrace();
        } finally {//关闭连接
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
        return list;
    }
*/
}
