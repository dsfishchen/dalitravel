package com.dalitravel.bigdataservice.repository;

import com.dalitravel.bigdataservice.repository.common.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.Integer.parseInt;
import static java.math.BigInteger.*;

@Service
public class ExecSqlDao {

    @Autowired
    private Environment env;
    @Autowired
    private MysqlDao mh;
/*
    @Autowired
    private SqliteHandle sh;
    @Autowired
    private VerticaHandle vh;
    @Autowired
    private ClickhouseHandle ch;
*/
    public List<Map<String, Object>> execSql(String dbtype,String sql){
        List ls = new ArrayList<Map<String, Object>>();
        if(dbtype.equals("mysqlExec"))
            return mh.execSql(sql);

        Connection connection = null;
        Statement statement = null;
        ResultSet results = null;
        int result = -1;
        try {
            if (dbtype.equals("sqliteUpdate")) {
                String slhost = env.getProperty("spring.datasource.sqlite.url");
                Class.forName("org.sqlite.JDBC");
                connection = DriverManager.getConnection(slhost);
                statement = connection.createStatement();
                result = statement.executeUpdate(sql);
                ls.add(new HashMap().put("result",result));
            } else if (dbtype.equals("jinguiMysqlUpdate")) {
                String URL = env.getProperty("jinguimysql.url");
                String USERNAME = env.getProperty("jinguimysql.user");
                String PASSWORD = env.getProperty("jinguimysql.password");
                Class.forName("com.mysql.jdbc.Driver");
                connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
                statement = connection.createStatement();
                result = statement.executeUpdate(sql);
                ls.add(new HashMap().put("result",result));
            } else {
                switch (dbtype) {
                    case "clickhouseSearch":
                        //connection = ClickhousePool.getPool().getConnection();
                        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
                        connection = DriverManager.getConnection("jdbc:clickhouse://192.168.1.24:8123/bigdata?password=BIGDATAcomedali8896927");
                        break;
                    case "sqliteSearch":
                        String slhost = env.getProperty("spring.datasource.sqlite.url");
                        Class.forName("org.sqlite.JDBC");
                        connection = DriverManager.getConnection(slhost);
                        break;
                    case "verticaExec":
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
                        Class.forName("com.vertica.jdbc.Driver");
                        connection = DriverManager.getConnection(url, myProp);
                        break;
                    case "jinguiMysqlQuery":
                        String URL = env.getProperty("jinguimysql.url");
                        String USERNAME = env.getProperty("jinguimysql.user");
                        String PASSWORD = env.getProperty("jinguimysql.password");
                        Class.forName("com.mysql.jdbc.Driver");
                        connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
                        break;
                }
                //connection.setNetworkTimeout(100);
                statement = connection.createStatement();
                statement.setQueryTimeout(100);
                results = statement.executeQuery(sql);
                ResultSetMetaData rsmd = results.getMetaData();
                while (results.next()) {
                    Map map = new HashMap();
                    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                        map.put(rsmd.getColumnName(i), results.getString(rsmd.getColumnName(i)));
                    }
                    ls.add(map);
                }
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
        return ls;
    }

    /**
     * 大批量数据导入时使用
     * 速度难以保证，暂时不使用。
     * @param dbtype
     * @param sql
     * @param tablename
     * @param list
     * @return
     */
    public boolean insertBigData(String dbtype,String sql,String tablename,List<Map<String,Object>> list) {
        boolean flag = true;
        Connection conn = null;
        PreparedStatement pstm = null;
        int result = -1;
        try {
            if (dbtype.equals("jinguiMysqlUpdate")) {
                String URL = env.getProperty("jinguimysql.url");
                String USERNAME = env.getProperty("jinguimysql.user");
                String PASSWORD = env.getProperty("jinguimysql.password");
                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
                pstm = conn.prepareStatement(sql);
                conn.setAutoCommit(false);

                int i = 0;
                int batchSize = 5000;//设置批量处理的数量

                for(Map<String,Object> map:list){
                    if(tablename.equals("customer_provice_data")){
                        //赋值insert into customer_provice_data(c_date,hotel_code,provice_code,nums) values (?,?,?,?)
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");//yyyy-mm-dd, 会出现时间不对, 因为小写的mm是代表: 秒
                        java.util.Date utilDate = sdf.parse(map.get("c_date").toString());
                        pstm.setDate(1, new java.sql.Date(utilDate.getTime()));
                        pstm.setLong(2, Long.parseLong(map.get("hotel_code").toString()));
                        pstm.setString(3, map.get("provice_code").toString());
                        pstm.setInt(4, parseInt(map.get("nums").toString()));
                        //添加到同一个批处理中
                        pstm.addBatch();
                    }else if(tablename.equals("customer_age_data")){
                        //赋值insert into customer_age_data(c_date,hotel_code,age_code,nums) values (?,?,?,?)
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");//yyyy-mm-dd, 会出现时间不对, 因为小写的mm是代表: 秒
                        java.util.Date utilDate = sdf.parse(map.get("c_date").toString());
                        pstm.setDate(1, new java.sql.Date(utilDate.getTime()));
                        pstm.setInt(2, parseInt(map.get("hotel_code").toString()));
                        pstm.setString(3, map.get("age_code").toString());
                        pstm.setInt(4, parseInt(map.get("nums").toString()));
                        //添加到同一个批处理中
                        pstm.addBatch();
                    }
                    i++;
                    if ( i % batchSize == 0 ) {
                        pstm.executeBatch();
                        conn.commit();
                    }
                }
                if ( i % batchSize != 0 ) {
                    pstm.executeBatch();
                    conn.commit();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {//关闭连接
            try {
                if (pstm != null) {
                    pstm.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return flag;
    }


    /*
    public List<Map> execSql1(String dbtype,String sql){
        List ls = new ArrayList<Map>();
        switch (dbtype){
            case "clickhouseSearch":
                ls = ch.execSql(sql);
                break;
            case "mysqlExec":
                ls = mh.execSql(sql);
                break;
            case "sqliteSearch":
                ls = sh.execSql(sql);
                break;
            case "verticaExec":
                ls = vh.execSql(sql);
                break;
        }
        return ls;
    }

    public List<Map> execclickhouseSql(String sql){

        List<Map> list = new ArrayList<Map>();

        String chhost = env.getProperty("clickhouse.Host");
        String chpwd = env.getProperty("clickhouse.Password");
        String chport = env.getProperty("clickhouse.Port");
        String chdb = env.getProperty("clickhouse.db");
        String address = "jdbc:clickhouse://" + chhost + ":" + chport + "/" + chdb + "?password=" + chpwd;

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
    */
}
