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
public class ClickhouseDao {

    @Autowired
    private Environment env;
/*
    public List<Map> execSql2(String sql) {

        List<Map> list = new ArrayList<Map>();

        String chhost = env.getProperty("clickhouse.Host");
        String chpwd = env.getProperty("clickhouse.Password");
        String chport = env.getProperty("clickhouse.Port");
        String chdb = env.getProperty("clickhouse.db");
        //String chsql = sql.replace(" ","%20");


        String address = "jdbc:clickhouse://" + chhost + ":" + chport + "/" + chdb + "?password=" + chpwd;
        //String address = "jdbc:clickhouse://192.168.1.24:8123/bigdata?password=BIGDATAcomedali8896927";

        //sql = "select * from bigdata.virtual_data WHERE COLLECTION_YEAR=1996 AND COLLECTION_MONTH=3 AND COLLECTION_DATE='1996-03-01' AND VIRTUAL_TYPE=142";

        Connection connection = null;
        Statement statement = null;
        ResultSet results = null;
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            connection = DriverManager.getConnection(address);
            statement = connection.createStatement();
            results = statement.executeQuery(sql);
            ResultSetMetaData rsmd = results.getMetaData();
            while (results.next()) {
                Map map = new HashMap();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    map.put(rsmd.getColumnName(i), results.getString(rsmd.getColumnName(i)));
                }
                list.add(map);
            }
        } catch (Exception e) {
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


    public List<Map> execSql(String sql) {

        List<Map> list = new ArrayList<Map>();

        Statement statement = null;
        ResultSet results = null;
        try {
            statement = ClickhousePool.getPool().getConnection().createStatement();
            results = statement.executeQuery(sql);
            ResultSetMetaData rsmd = results.getMetaData();
            while (results.next()) {
                Map map = new HashMap();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    map.put(rsmd.getColumnName(i), results.getString(rsmd.getColumnName(i)));
                }
                list.add(map);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {//关闭连接
            try {
                if (results != null) {
                    results.close();
                }
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return list;
    }
*/
}


