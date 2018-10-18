package com.dalitravel.bigdataservice.repository.sql;

import org.springframework.stereotype.Service;

@Service
public class TouristpropertySql {

    /**
     * 年龄分布
     */
    public String getAgeDistributionType = "sqliteSearch";
    public String getAgeDistributionSql(){
        String sql = "select id as c_id,age_name,percent from age_data order by percent desc;";
        return sql;
    }

    /**
     * 性别分布-进入大理
     */
    public String getSexDistributionType = "sqliteSearch";
    public String getSexDistributionSql(){
        String sql = "select id as c_id,sex_name,percent from sex_data order by percent desc;";
        return sql;
    }
    /**
     * 性别分布-搜索
     */
    public String getSexofFindType = "sqliteSearch";
    public String getSexofFindSql(){
        String sql = "select id as c_id,sex_name,percent from sex_data_find order by percent desc;";
        return sql;
    }

    /**
     * 游客来源
     */
    public String getTouristSourceType = "verticaExec";
    public String getTouristSourceSql(){
        String sql = "select user_province,count(*) as nums,(select count(*) from wifi_data where user_province is not null and user_province<>'*' and user_city<>'大理') as allnums from wifi_data where user_province is not null and user_province<>'*' and user_city<>'大理' group by user_province,allnums order by nums desc limit 30";
        return sql;
    }

    /**
     * 学历分布
     */
    public String getEducationType = "sqliteSearch";
    public String getEducationSql(){
        String sql = "select grade,num as nums,(select sum(num) from education_data) as allnums from education_data order by num desc";
        return sql;
    }




}
