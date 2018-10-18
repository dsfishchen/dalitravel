package com.dalitravel.bigdataservice.repository.sql;

import org.springframework.stereotype.Service;

@Service
public class StatisticsSql {

    /**
     * 每天定时任务，将昨天之前的游客数据统计到sqlite表中。
     */
    public String getTouristType = "clickhouseSearch";
    public String getTouristSql(String startdate,String enddate,String mac){
        String sql = "select equipment_mac,c_date,c_hour,count(*) as c_nums \n" +
                "from (select TERMINAL_MAC,max(COLLECTION_EQUIPMENT_MAC) AS equipment_mac,max(CAPTURE_TIME)-min(CAPTURE_TIME) as time,toDate(max(CAPTURE_TIME)) AS c_date,floor(toHour(toDateTime(max(CAPTURE_TIME)))/2)*2 AS c_hour\n" +
                "\tfrom bigdata.terminal_data \n" +
                "\twhere  COLLECTION_DATE>=toDate('"+startdate+"') \n" +
                "\t\tand  COLLECTION_DATE<=toDate('"+enddate+"') \n" +
                "and COLLECTION_EQUIPMENT_MAC in (" + mac + ")"+
                "\t\tand TERMINAL_MAC not in (select TERMINAL_MAC from bigdata.local_data where COLLECTION_DATE in (select max(COLLECTION_DATE) as COLLECTION_DATE from bigdata.local_data)) \n" +
                "\tgroup by TERMINAL_MAC having time>7200) \n" +
                "GROUP BY equipment_mac,c_date,c_hour ORDER BY c_date,c_hour;";
        return sql;
    }

    /**
     * 获取当前已统计数据的最后日期
     */
    public String getLastdateType = "sqliteSearch";
    public String getLastdateSql(){
        String sql = "select max(c_date) as c_date from statistics_tourist";
        return sql;
    }


    /**
     * 判断是否存在已统计的记录
     */
    public String checkStatisticsType = "sqliteSearch";
    public String checkStatisticsSql(String startdate,String enddate){
        String sql = "select count(*) as tn from statistics_tourist where date(c_date)>=date('"+startdate+"') and date(c_date)<=date('"+enddate+"');";
        return sql;
    }
    /**
     * 按日期删除记录
     */
    public String delStatisticsType = "sqliteUpdate";
    public String delStatisticsSql(String startdate,String enddate){
        String sql = "delete from statistics_tourist where date(c_date)>=date('"+startdate+"') and date(c_date)<=date('"+enddate+"');";
        return sql;
    }
}
