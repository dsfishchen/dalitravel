package com.dalitravel.bigdataservice.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dalitravel.bigdataservice.BigdataserviceApplicationTests;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class JinGuiByProviceTest extends BigdataserviceApplicationTests {

    @Autowired
    StatisticsService ss;
    @Autowired
    FlowmeterService fs;
    @Autowired
    BehaviorService bs;
    @Autowired
    StatisticsService ss1;
    @Autowired
    JinGuiHotelService js;

    @Test
    public void statisticsTourist() {

        ss.statisticsTourist();
        Date now = new Date();
        now.getYear();
    }

    @Test
    public void myget(){

        Calendar starttime = Calendar.getInstance();
        Calendar endtime = Calendar.getInstance();
        starttime.set(2018,8,18);
        endtime.set(2018,8,18);
        Long startTime = starttime.getTimeInMillis();
        Long endTime = endtime.getTimeInMillis();
        Long oneDay = 1000 * 60 * 60 * 24l;
        Long loopTime = startTime;
        while(loopTime<=endTime){
            Date dd = new Date(loopTime);
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            getData(df.format(dd));
            loopTime += oneDay;
        }

    }

    public void getData(String today) {
        String url = "http://116.55.233.251:8080/DataTotal/api/Data/getTotal";
        Map map = new HashMap();
        map.put("intime",today);
        map.put("company","dlzhlyc");
        map.put("key","D878B1CF0712D722DEFEBC");
        map.put("dtype","getPtTotal");

        try{
            DefaultHttpClient httpClient = new DefaultHttpClient();
            HttpPost httpPost = new HttpPost(url);
            httpPost.addHeader(HTTP.CONTENT_TYPE, "application/json");
            String jsonstr = JSON.toJSONString(map);
            StringEntity se = new StringEntity(jsonstr);
            se.setContentType("text/json");
            se.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE, "application/json"));
            httpPost.setEntity(se);
            HttpResponse response=httpClient.execute(httpPost);
            //输出调用结果
            if(response != null && response.getStatusLine().getStatusCode() == 200) {

                String result= EntityUtils.toString(response.getEntity());
                // 生成 JSON 对象
                JSONObject obj = JSONObject.parseObject(result);
                String str = obj.get("data").toString();
                System.out.println(obj);
                if(str==null || str.equals("[]"))
                    return;
                List<Map> ts = (List<Map>) JSONArray.parseArray(str,map.getClass());
                //setCustomer(today,ts);
            }
        } catch (Exception e) {
            System.out.println("执行失败！" );
        }
    }

    public int setCustomer(String date,List<Map> list){

        // 定义数据库的用户名
        String USERNAME = "root";
        // 定义数据库的密码
        String PASSWORD = "Root2016!";
        // 定义数据库的驱动信息
        String DRIVER = "com.mysql.jdbc.Driver";
        // 定义访问数据库的地址
        String URL = "jdbc:mysql://101.37.148.119:3306/hoteldata";

        // 定义访问数据库的连接
        Connection connection = null;
        // 定义sql语句的执行对象
        PreparedStatement pstmt = null;

        // 定义查询返回的结果集合
        //ResultSet resultSet;
        int result = -1;

        //insert into customer_data(c_date,sex,totalnumber,hotelcode) values (STR_TO_DATE('1992-04-12','%Y-%m-%d'),1,10,'532901200526329')
        String sql = "insert into customer_data(c_date,sex,totalnumber,hotelcode) values";
        for(Map map:list) {
            sql += "(STR_TO_DATE('" + date + "','%Y-%m-%d')"
                    + ",'" +map.get("sex").toString() + "'"
                    + "," +map.get("totalnumber").toString()
                    + ",'" +map.get("hotelcode").toString()+"')"
                    + ",";
        }
        sql = sql.substring(0,sql.length() - 1);
        try {
            Class.forName(DRIVER);
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            pstmt = connection.prepareStatement(sql);
            result = pstmt.executeUpdate();
            /*for(Map map:list){
                String upstr = sql +" (STR_TO_DATE('" + date + "','%Y-%m-%d')"
                        + "," +map.get("sex").toString()
                        + "," +map.get("totalnumber").toString()
                        + ",'" +map.get("hotelcode").toString()+"')";
                pstmt = connection.prepareStatement(upstr);
                result = pstmt.executeUpdate();
            }*/
        } catch (Exception e) {
            e.printStackTrace();
        } finally {//关闭连接
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Test
    public void sss(){

        bs.updatelocusnumDate();
        ss.statisticsTourist();
        js.updateCustomerData();
    }
}