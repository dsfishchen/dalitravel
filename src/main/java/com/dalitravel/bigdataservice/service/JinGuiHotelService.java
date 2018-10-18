package com.dalitravel.bigdataservice.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dalitravel.bigdataservice.repository.ExecSqlDao;
import com.dalitravel.bigdataservice.repository.sql.JinGuiHotelSql;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class JinGuiHotelService {

    @Autowired
    private ExecSqlDao ess;
    @Autowired
    private JinGuiHotelSql jgs;

    public List<Map<String, Object>> getCityHotelNum(){
        List list = new ArrayList();



        return list;
    }

    public List<Map<String, Object>> getAreaHotelNum(){
        List list = new ArrayList();



        return list;
    }

    public List<Map<String, Object>> getAreaLeavingHotelNum(){
        List list = new ArrayList();



        return list;
    }

    public List<Map<String, Object>> getOnephonetravelHotelNum(){
        List list = new ArrayList();



        return list;
    }

    /**
     * 性别分布
     * @return sex_name,percent
     */
    public List<Map<String, Object>> getSexDistribution(){
        //首先更新金圭的数据
        //updateCustomerData();

        List<Map<String, Object>> list = ess.execSql(jgs.getSexType,jgs.getSexSql());
        int allnum = 0;
        for(Map map:list){
            allnum += Integer.parseInt(map.get("nums").toString());
        }
        for(Map map:list){
            if(map.get("sex").toString().equals("1"))
                map.put("sex_name","男");
            else
                map.put("sex_name","女");
            int tn = Integer.parseInt(map.get("nums").toString());
            map.put("percent", (new DecimalFormat("0.00").format(((double)tn*100)/allnum))+"%");
        }
        return list;
    }
    /**
     * 游客来源，省份数据
     * @return
     */
    public List<Map<String, Object>> getTouristProvice(){
        List<Map<String, Object>> list = ess.execSql(jgs.getProviceType,jgs.getProviceSql());
        int allnums = 0;
        for(Map map:list){
            allnums += Integer.parseInt(map.get("nums").toString());
        }
        for(Map map:list){
            map.put("allnums",allnums);
            int num = Integer.parseInt(map.get("nums").toString());
            map.put("percent",(new DecimalFormat("0.00").format(((double)num*100)/allnums))+"%");
        }
        return list;
    }
    /**
     * 游客来源，城市
     * @return
     */
    public List<Map<String, Object>> getTouristCity(){
        List<Map<String, Object>> list = ess.execSql(jgs.getTouristCityType,jgs.getTouristCitySql());
        int allnums = Integer.parseInt(list.get(0).get("allnums").toString());
        for(Map map:list){
            map.put("allnums",allnums);
            int num = Integer.parseInt(map.get("nums").toString());
            map.put("percent",(new DecimalFormat("0.00").format(((double)num*100)/allnums))+"%");
        }
        return list;
    }
    /**
     * 游客age
     * @return
     */
    public List<Map<String, Object>> getTouristAge(){
        List<Map<String, Object>> list = ess.execSql(jgs.getAgeType,jgs.getAgeSql());
        int allnums = 0;
        for(Map map:list){
            allnums += Integer.parseInt(map.get("nums").toString());
        }
        for(Map map:list){
            map.put("allnums",allnums);
            int num = Integer.parseInt(map.get("nums").toString());
            map.put("percent",(new DecimalFormat("0.00").format(((double)num*100)/allnums))+"%");
        }
        return list;
    }

    /**
     * 从金圭获取3天前的入住数据。金圭答复仅可获取3天前信息，近2天的数据不准确。比如今天10号，那么7号数据是准的，但是8/9/10号数据不准。
     * 由前台提交查数申请时触发更新操作。
     * 每天9：00-9:03；12:00-12:03；16:00-16:03更新三次。首次访问时若无当天数据，直接更新。
     * @return
     */
    public boolean checkupdatetime(){
        boolean flag = true;
        Calendar time = Calendar.getInstance();
        int hour = time.get(Calendar.HOUR_OF_DAY);
        int mintue = time.get(Calendar.MINUTE);
        if((hour==9 || hour == 12 || hour == 14) && mintue<3)
            flag = true;
        else
            flag = false;
        return flag;
    }

    public boolean updateCustomerData(){
        boolean flag = true;
        //获取当前已统计数据的最后日期
        List<Map<String, Object>> lastlist = ess.execSql(jgs.getLastSexDateType,jgs.getLastSexDateSql());
        //分成几种情况，比如今天为x月10日
        //1、lastdate=today，说明3天前的数据已经更新过，只需要更新仅8/9/10这三天的数据。
        //2、lastdate=today-1，说明今天还没更新，但是昨天更新过，所以6号的数据是准的，但是7-9不准，需要更新7-10号的数据。
        //3、lastdate<today-1，说明好几天没有更新，需要更新lastdate-3到today的数据。
        //总结：需要获取lastdate-3到today的数据。
        String startdate = ((Map) lastlist.get(0)).get("c_date").toString();
        Calendar starttime = Calendar.getInstance();
        Calendar endtime = Calendar.getInstance();
        Calendar lasttime = Calendar.getInstance();
        endtime.set(endtime.get(Calendar.YEAR), endtime.get(Calendar.MONTH), endtime.get(Calendar.DATE));
        //starttime.set(Integer.parseInt(startdate.split("-")[0]), Integer.parseInt(startdate.split("-")[1]) - 1, Integer.parseInt(startdate.split("-")[2]));
        //starttime.add(Calendar.DAY_OF_YEAR,-3);
        Date dateTime = new Date();
        dateTime.setYear(Integer.parseInt(startdate.split("-")[0]));
        dateTime.setMonth(Integer.parseInt(startdate.split("-")[1])-1);
        dateTime.setDate(Integer.parseInt(startdate.split("-")[2]));
        lasttime.set(dateTime.getYear(),dateTime.getMonth(),dateTime.getDate());

        dateTime = new Date(dateTime.getTime() - 2 * 24 * 60 * 60 * 1000L);//从已统计日期的三天前开始补充统计数据
        starttime.set(dateTime.getYear(),dateTime.getMonth(),dateTime.getDate());

        Long startTime = starttime.getTimeInMillis();
        Long endTime = endtime.getTimeInMillis();
        Long lastTime = lasttime.getTimeInMillis();

        Long oneDay = 1000 * 60 * 60 * 24l;
        Long looptime = startTime;

        /*
        if (lastTime >= endTime  && !checkupdatetime())
            return true;
        */
        if (lastTime >= endTime)//如果当天更新过，就不再更新
            return true;

        while (looptime <= endTime) {
            Date dd = new Date(looptime);
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            updateJinGuiHotel(df.format(dd),"getSextotal");
            updateJinGuiHotel(df.format(dd),"getProviencetotal");
            updateJinGuiHotel(df.format(dd),"getPtTotal");
            looptime += oneDay;
        }
        return flag;
    }
    /**
     * 根据日期从金圭获取数据，并将数据更新到本地mysql数据库中。
     * dtype:数据更新的类型
     *      getSextotal 按性别
     *      getProviencetotal   按省份
     *      getPtTotal  按年龄
     */
    public boolean updateJinGuiHotel(String data,String dtype){
        boolean flag = true;

        //首先从金圭获取数据
        String url = "http://116.55.233.251:8080/DataTotal/api/Data/getTotal";
        Map map = new HashMap();
        map.put("intime",data);
        map.put("company","dlzhlyc");
        map.put("key","D878B1CF0712D722DEFEBC");
        //map.put("dtype","getSextotal");
        map.put("dtype",dtype);

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
                if(str==null || str.equals("[]"))
                    return true;
                List<Map<String, Object>> ts = (List<Map<String, Object>>) JSONArray.parseArray(str,map.getClass());

                //更新数据时先删除本地数据库中data的数据，然后再将金圭数据更新到数据库中
                switch (dtype){
                    case "getSextotal":
                        ess.execSql(jgs.delSexType,jgs.delSexSql(data));
                        setCustomer(data,ts);
                        break;
                    case "getProviencetotal":
                        ess.execSql(jgs.delProviceType,jgs.delProviceSql(data));
                        setCustomerProvice(data,ts);
                        break;
                    case "getPtTotal":
                        ess.execSql(jgs.delAgeType,jgs.delAgeSql(data));
                        setCustomerAge(data,ts);
                        break;
                }
            }
        } catch (Exception e) {
            System.out.println("执行失败！" );
        }
        return flag;
    }

    /**
     * 更新顾客性别信息
     * @param date
     * @param list
     * @return
     */
    public synchronized boolean setCustomer(String date,List<Map<String, Object>> list){
        //sql拼接，不在集中文件处理。
        String sql = "insert into customer_data(c_date,sex,totalnumber,hotelcode) values";
        for(Map map:list) {
            sql += "(STR_TO_DATE('" + date + "','%Y-%m-%d')"
                    + ",'" +map.get("sex").toString() + "'"
                    + "," +map.get("totalnumber").toString()
                    + ",'" +map.get("hotelcode").toString()+"')"
                    + ",";
        }
        sql = sql.substring(0,sql.length() - 1);
        ess.execSql(jgs.insertSexType,sql);
        return true;
    }

    /**
     * 更新顾客省份信息
     * @param date
     * @param list
     * @return
     */
    public synchronized boolean setCustomerProvice(String date,List<Map<String, Object>> list){

        int i = 0;
        int batchSize = 200;//设置批量处理的数量，每获取到200个客栈的数据时更新一次数据库
        String sql = "insert into customer_provice_data(c_date,hotel_code,provice_code,nums) values";

        for(Map<String, Object> map:list) {
            String hotel_code=map.get("hotelcode").toString();
            map.remove("hotelcode");
            map.remove("totalnumber");
            for(Map.Entry<String, Object> entry:map.entrySet()){
                String nums = entry.getValue().toString();
                if(nums.equals("") || nums==null || nums.equals("null"))
                    nums = "0";
                sql += "(STR_TO_DATE('" + date + "','%Y-%m-%d')"
                        + "," + hotel_code
                        + ",'" + entry.getKey() + "'"
                        + "," + nums + ")"
                        + ",";
            }
            i++;
            if ( i % batchSize == 0 ) {
                sql = sql.substring(0,sql.length() - 1);
                ess.execSql(jgs.insertProviceType,sql);
                sql = "insert into customer_provice_data(c_date,hotel_code,provice_code,nums) values";
            }
        }
        if ( i % batchSize != 0 ) {
            sql = sql.substring(0, sql.length() - 1);
            ess.execSql(jgs.insertProviceType, sql);
        }
        return true;
        //sql拼接，不在集中文件处理。
        /*
        for(Map<String, Object> map:list) {
            String sql = "insert into customer_provice_data(c_date,hotel_code,provice_code,nums) values";
            String hotel_code=map.get("hotelcode").toString();
            map.remove("hotelcode");
            map.remove("totalnumber");
            for(Map.Entry<String, Object> entry:map.entrySet()){
                String nums = entry.getValue().toString();
                if(nums.equals("") || nums==null || nums.equals("null"))
                    nums = "0";
                sql += "(STR_TO_DATE('" + date + "','%Y-%m-%d')"
                        + "," + hotel_code
                        + ",'" + entry.getKey() + "'"
                        + "," + nums + ")"
                        + ",";
            }
            sql = sql.substring(0,sql.length() - 1);
            ess.execSql(jgs.insertProviceType,sql);
        }*/
        /*
        List<Map<String, Object>> list1 = new ArrayList<Map<String, Object>>();
        for(Map<String, Object> map:list) {
            String hotel_code=map.get("hotelcode").toString();
            map.remove("hotelcode");
            map.remove("totalnumber");
            for(Map.Entry<String, Object> entry:map.entrySet()){
                Map<String, Object> map1 = new HashMap<String, Object>();
                map1.put("c_date",date);
                map1.put("hotel_code",hotel_code);
                map1.put("provice_code",entry.getKey());
                String nums = entry.getValue().toString();
                if(nums.equals("") || nums==null || nums.equals("null"))
                    nums = "0";
                map1.put("nums",nums);
                list1.add(map1);
            }
        }
        ess.insertBigData(jgs.insertProviceType,jgs.insertProviceSql(),"customer_provice_data",list1);
        return true;
        */
    }

    /**
     * 更新顾客年龄段信息
     * @param date
     * @param list
     * @return
     */

    public synchronized boolean setCustomerAge(String date,List<Map<String, Object>> list){

        int i = 0;
        int batchSize = 200;//设置批量处理的数量，每获取到200个客栈的数据时更新一次数据库
        String sql = "insert into customer_age_data(c_date,hotel_code,age_code,nums) values";

        for(Map<String, Object> map:list) {
            String hotel_code=map.get("hotelcode").toString();
            map.remove("hotelcode");
            map.remove("totalnumber");
            for(Map.Entry<String, Object> entry:map.entrySet()){
                String nums = entry.getValue().toString();
                if(nums.equals("") || nums==null || nums.equals("null"))
                    nums = "0";
                sql += "(STR_TO_DATE('" + date + "','%Y-%m-%d')"
                        + "," + hotel_code
                        + ",'" + entry.getKey() + "'"
                        + "," + nums + ")"
                        + ",";
            }
            i++;
            if ( i % batchSize == 0 ) {
                sql = sql.substring(0,sql.length() - 1);
                ess.execSql(jgs.insertAgeType,sql);
                sql = "insert into customer_age_data(c_date,hotel_code,age_code,nums) values";
            }
        }
        if ( i % batchSize != 0 ) {
            sql = sql.substring(0, sql.length() - 1);
            ess.execSql(jgs.insertAgeType, sql);
        }
        return true;
    }
}
