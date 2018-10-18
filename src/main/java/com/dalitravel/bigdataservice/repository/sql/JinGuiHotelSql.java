package com.dalitravel.bigdataservice.repository.sql;

import org.springframework.stereotype.Service;

@Service
public class JinGuiHotelSql {

    /**
     * 获取已同步的最新时间
     */
    public String getLastSexDateType = "jinguiMysqlQuery";
    public String getLastSexDateSql(){
        String sql = "select max(c_date) as c_date from customer_data";
        return sql;
    }

    /********************************************************
     * 性别
     * 根据日期删除数据
     */
    public String delSexType = "jinguiMysqlUpdate";
    public String delSexSql(String date){
        String sql = "delete from customer_data where c_date=STR_TO_DATE('"+date+"','%Y-%m-%d');";
        return sql;
    }
    /**
     * 将金圭男女比例数据插入数据库
     */
    public String insertSexType = "jinguiMysqlUpdate";
    //插入的sql语句需要拼接，不在集中文件处理。
    /**
     * 获取男女比例数据
     */
    public String getSexType = "jinguiMysqlQuery";
    public String getSexSql(){
        String sql = "select sex,sum(totalnumber) as nums from customer_data group by sex having sex in ('1','2')";
        return sql;
    }
    /*****************************************************
    * 城市
     * 游客来源
     */
    public String getTouristCityType = "verticaExec";
    public String getTouristCitySql(){
        String sql = "select user_city,count(*) as nums,(select count(*) from wifi_data where user_city is not null and user_city<>'*' and user_city<>'大理') as allnums from wifi_data where user_city is not null and user_city<>'*' and user_city<>'大理' group by user_city,allnums order by nums desc limit 30";
        return sql;
    }

    /*******************************************************
     * 省份
     * 根据日期删除数据
     */
    public String delProviceType = "jinguiMysqlUpdate";
    public String delProviceSql(String date){
        String sql = "delete from customer_provice_data where c_date=STR_TO_DATE('"+date+"','%Y-%m-%d')";
        return sql;
    }
    /**
     * 将金圭数据插入数据库
     */
    public String insertProviceType = "jinguiMysqlUpdate";
    public String insertProviceSql(){
        String sql = "insert into customer_provice_data(c_date,hotel_code,provice_code,nums) values (?,?,?,?)";
        return sql;
    }
    /**
     * 获取比例数据
     */
    public String getProviceType = "jinguiMysqlQuery";
    public String getProviceSql(){
        String sql = "select b.code,b.name as user_province,a.nums from (select provice_code,sum(nums) as nums from customer_provice_data a group by provice_code) as a,provice_code as b where a.provice_code=b.code order by a.nums desc";
        //sql = "select b.code,b.name as user_province,a.nums from (select provice_code,sum(nums) as nums from customer_provice_data a group by provice_code) as a left join provice_code as b on a.provice_code=b.code order by a.nums desc";
        return sql;
    }


    /***********************************************************
     * 年龄
     * 根据日期删除数据
     */
    public String delAgeType = "jinguiMysqlUpdate";
    public String delAgeSql(String date){
        String sql = "delete from customer_age_data where c_date=STR_TO_DATE('"+date+"','%Y-%m-%d');";
        return sql;
    }
    /**
     * 将金圭数据插入数据库
     */
    public String insertAgeType = "jinguiMysqlUpdate";
    //插入的sql语句需要拼接，不在集中文件处理。
    /**
     * 获取比例数据
     */
    public String getAgeType = "jinguiMysqlQuery";
    public String getAgeSql(){
        String sql = "select b.name as age_name,b.code as c_id,a.nums from (select age_code,sum(nums) as nums from customer_age_data group by age_code) as a,age_code as b where a.age_code=b.code";
        return sql;
    }


}
