package com.dalitravel.bigdataservice.repository.sql;

import org.springframework.stereotype.Service;

@Service
public class FlowmeterSql {

    public String getAllTouristType = "clickhouseSearch";
    public String getAllTouristSql(String mac){
        //原来的sql，留存
        String sql = "select count(*) as nums from (select TERMINAL_MAC,max(CAPTURE_TIME)-min(CAPTURE_TIME) as time from bigdata.terminal_data where  COLLECTION_DATE=toDate(now()) and COLLECTION_EQUIPMENT_MAC in ("+mac+")  and TERMINAL_MAC not in (select TERMINAL_MAC from bigdata.local_data where COLLECTION_DATE in (select max(COLLECTION_DATE) as COLLECTION_DATE from bigdata.local_data)) group by TERMINAL_MAC having time>7200)";
        return sql;
    }
    /**
     * 从clickhouse数据库，根据mac地址获取游客连接数量。
     * 由于原大数据平台是依据用户终端链接时长超过2小时来计算游客数量，跨wifi也计算在内，所以无法按wifi地址进行统计，只能逐个获取。
     * 返回：wifi地址equipment_mac、游客数量nums
     */
    public String getCKTouristType = "clickhouseSearch";
    public String getCKTouristSql(String mac){
        //原来的sql，留存
        String sql = "select equipment_mac,count(*) as nums from (select TERMINAL_MAC,max(COLLECTION_EQUIPMENT_MAC) AS equipment_mac,max(CAPTURE_TIME)-min(CAPTURE_TIME) as time from bigdata.terminal_data where  COLLECTION_DATE=toDate(now())"
                + " and COLLECTION_EQUIPMENT_MAC in (" + mac + ")"
                + " and TERMINAL_MAC not in (select TERMINAL_MAC from bigdata.local_data where COLLECTION_DATE in (select max(COLLECTION_DATE) as COLLECTION_DATE from bigdata.local_data)) group by TERMINAL_MAC having time>7200) GROUP BY equipment_mac;";
        sql = "select equipment_mac,count(*) as nums from (select TERMINAL_MAC,max(COLLECTION_EQUIPMENT_MAC) AS equipment_mac,max(CAPTURE_TIME)-min(CAPTURE_TIME) as time from bigdata.terminal_data where  COLLECTION_DATE=toDate(now())"
                + " and TERMINAL_MAC not in (select TERMINAL_MAC from bigdata.local_data where COLLECTION_DATE in (select max(COLLECTION_DATE) as COLLECTION_DATE from bigdata.local_data)) group by TERMINAL_MAC having time>7200) GROUP BY equipment_mac;";
        return sql;
    }
    /**
     * 删除多余的游客数据
     */
    public String delTouristType = "sqliteUpdate";
    public String delTouristSql(){
        String sql = "delete from touristnum_date where c_flag=0";
        return sql;
    }
    /**
     * 将clickhouse中的游客数据保存到sqlite数据库中,状态c_flag置为2
     */
    public String setTouristType = "sqliteUpdate";
    public String setTouristSql(String values){
        String sql = "insert into touristnum_date(equipment_mac,nums,c_flag) values " + values;
        return sql;
    }
    /**
     * 将现有游客数据状态置为中间状态
     */
    public String setTouristFlagType = "sqliteUpdate";
    public String setTouristFlagSql(){
        String sql = "update touristnum_date set c_flag=0 where c_flag=1";
        return sql;
    }
    /**
     * 将最新游客数据状态置为1
     */
    public String setLastTouristFlagType = "sqliteUpdate";
    public String setLastTouristFlagSql(){
        String sql = "update touristnum_date set c_flag=1 where c_flag=2";
        return sql;
    }
    /**
     * 从sqlite数据库，根据mac地址获取游客连接数量。
     */
    public String getTouristType = "sqliteSearch";
    public String getTouristSql(String mac){
        String sql = "select equipment_mac,nums from touristnum_date a where a.c_flag in (select max(c_flag) from touristnum_date b where b.equipment_mac=a.equipment_mac)" +
                " and equipment_mac in (" + mac + ")";
        return sql;
    }


    /**
     * 说明：删除place_data.place_type not in (5,8,9)，这3个类型是高速出口、路口等摄像头。其中漾濞、祥云等县仅有路口的数据，若剔除，这些县就没有游客数据。
     * 原大数据平台的sql：SELECT equipment_mac from equipment_data as e where e.place_id in (select id as place_id from place_data where place_type not in (5,8,9))
     * 现更新调整后的sql：SELECT equipment_mac from equipment_data as e where e.place_id in (select id as place_id from place_data)
     * areaid:县市id
     * placeid:景点id
     */
    public String getAreaMacType = "sqliteSearch";
    public String getAreaMacSql(String area_type){
        String sql = "";
        if(area_type.equals("") || area_type==null)   //获取全州所有的wifi地址
            sql = "SELECT equipment_mac from equipment_data as e where e.place_id in (select id as place_id from place_data)";
        else if (area_type.equals("dali")){
            sql = "SELECT equipment_mac from equipment_data as e " +
                    "where e.place_id in (select id as place_id from place_data " +
                    "where place_type not in (5,8,9) and area_id in (select id as area_id from area_data where area_type in ('" + area_type + "')))";
        }
        else        //根据州县名称获取州县地址，在两个地方有使用：1在获取各县游客数量，2获取景点详细数据
            sql = "SELECT equipment_mac from equipment_data as e " +
                    "where e.place_id in (select id as place_id from place_data " +
                                            "where area_id in (select id as area_id from area_data where area_type in ('" + area_type + "')))";
        return sql;
    }
    public String getAllAreaMacSql(String area_type){
        String sql = "SELECT equipment_mac from equipment_data as e where e.place_id in (select id as place_id from place_data where place_type not in (5,8,9))";
        return sql;
    }

    /**
     * 根据县市获取景点清单，包括景点名称、景点id
     * 没有剔除高速路口的数据place_type not in (5,8,9) and
     */
    public String getPlaceByAreaType = "sqliteSearch";
    public String getPlaceByAreaSql(String areaid) {
        //String sql = "select place_name,id as place_id,latitude,longitude,place_type from place_data where area_id in (select id from area_data where area_type in ('" + areaid + "'))";
        String sql = "select place_name,id as place_id,latitude,longitude,place_type,(select group_concat(b.equipment_mac,\"',\") from equipment_data b where b.place_id=a.id group by b.place_id) as equipment_mac from place_data a where area_id in (select id from area_data where area_type in ('dali'));";
        return sql;
    }
    public String getPlaceByAreaSql1(String areaid) {
        String sql = "select equipment_mac,place_id,location as place_name,latitude,longitude,(select place_type from place_data a where a.id=b.place_id) place_type  " +
                "from equipment_data b where place_id in (" +
                "select id as place_id from place_data where area_id in (select id from area_data where area_type in ('" + areaid + "')))";
        return sql;
    }
    public String getPlaceByAreaSq2(String areaid) {
        String sql = "select place_name,id as place_id,latitude,longitude,place_type from place_data where area_id in (select id from area_data where area_type in ('" + areaid + "'))";
        //String sql = "select place_name,id as place_id,latitude,longitude,place_type,(select group_concat(b.equipment_mac,\"',\") from equipment_data b where b.place_id=a.id group by b.place_id) as equipment_mac from place_data a where area_id in (select id from area_data where area_type in ('dali'));";
        return sql;
    }

    /**
     * 获取位置的mac地址
     */
    public String getMacByPlaceType = "sqliteSearch";
    public String getMacByPlaceSql(String placeid) {
        String sql = "SELECT equipment_mac,place_id,location,latitude,longitude from equipment_data as e where e.place_id in ('" + placeid + "')";
        return sql;
    }

    /**
     * 获取大理州下所有县市的清单
     * 返回area_name,area_id,area_type
     */
    public String getAllAreaType = "sqliteSearch";
    public String getAllAreaSql(){
        String sql = "select area_type,group_concat(id,',') as area_id,group_concat(area_name) as area_name,group_concat(longitude) as longitude,group_concat(latitude) as latitude from area_data group by area_type";
        return sql;
    }

    /**
     * 按景区id获取景区的统计数据
     */
    public String getStatisticsTouristType = "sqliteSearch";
    public String getStatisticsTouristSql(String type,String place_id,String starttime,String endtime){
        String sql = "";
        String placesql = "";
        if(place_id==null || place_id.equals("")) {
            //placesql = " where place_id in (select id as place_id from place_data where place_type not in (5,8,9))";
            switch (type) {
                case "day":
                    sql = "select c_year,c_month,c_day,sum(c_nums) as c_nums from ( " +
                            " select a.nums as c_nums,strftime('%Y',date) as c_year,strftime('%m',date) as c_month,strftime('%d',date) as c_day from persons_data a where date(date)>=date('" + starttime + "') and date(date)<=date('" + endtime + "'))" +
                            " group by c_year,c_month,c_day order by c_year,c_month,c_day";
                    break;
                case "month":
                    sql = "select c_year,c_month,sum(c_nums) as c_nums from ( " +
                            " select a.nums as c_nums,strftime('%Y',date) as c_year,strftime('%m',date) as c_month,strftime('%d',date) as c_day from persons_data a where date(date)>=date('" + starttime + "') and date(date)<=date('" + endtime + "'))" +
                            " group by c_year,c_month order by c_year,c_month";
                    break;
                case "year":
                    sql = "select c_year,sum(c_nums) as c_nums from ( " +
                            " select a.nums as c_nums,strftime('%Y',date) as c_year,strftime('%m',date) as c_month,strftime('%d',date) as c_day from persons_data a where date(date)>=date('" + starttime + "') and date(date)<=date('" + endtime + "'))" +
                            " group by c_year order by c_year";
                    break;
            }
        }
        else {
            placesql = " where place_id in (" + place_id + ") and place_id in (select id as place_id from place_data where place_type not in (5,8,9))";
            switch (type) {
                case "day":
                /*sql="select c_date,c_hour,sum(c_nums) as c_nums from statistics_tourist where equipment_mac in (select equipment_mac from equipment_data " +
                        placesql+" ) and date(c_date)>=date('"+starttime+"') and date(c_date)<=date('"+endtime+"') group by c_date,c_hour order by c_hour";*/
                    sql = "select c_year,c_month,c_day,sum(c_nums) as c_nums from statistics_tourist where equipment_mac in (select equipment_mac from equipment_data " +
                            placesql + " ) and date(c_date)>=date('" + starttime + "') and date(c_date)<=date('" + endtime + "') group by c_year,c_month,c_day order by c_year,c_month,c_day";
                    break;
                case "month":
                    sql = "select c_year,c_month,sum(c_nums) as c_nums from statistics_tourist where equipment_mac in (select equipment_mac from equipment_data " +
                            placesql + " ) and date(c_date)>=date('" + starttime + "') and date(c_date)<=date('" + endtime + "') group by c_year,c_month order by c_year,c_month";
                    break;
                case "year":
                    sql = "select c_year,sum(c_nums) as c_nums from statistics_tourist where equipment_mac in (select equipment_mac from equipment_data " +
                            placesql + " ) and date(c_date)>=date('" + starttime + "') and date(c_date)<=date('" + endtime + "') group by c_year order by c_year";
                    break;
            }
        }
        return sql;
    }


    //根据macstr获取当日的时刻用户数据。
    public String getThisdayTouristType = "clickhouseSearch";
    public String getThisdayTouristSql() {
        /*String sql = "select place_id,c_hour,sum(c_nums) as c_nums " +
                " from statistics_tourist a,equipment_data b" +
                " where a.equipment_mac=b.equipment_mac" +
                " and date(c_date)=date('"+thisdate+"')" +
                " group by place_id,c_hour order by place_id,c_hour";*/
        //String sql = "select COLLECTION_EQUIPMENT_MAC,count(distinct TERMINAL_MAC) as nums from bigdata.terminal_data where COLLECTION_DATE=toDate(now()) GROUP BY COLLECTION_EQUIPMENT_MAC";
        String sql = "select  COLLECTION_EQUIPMENT_MAC,toHour(toDateTime(CAPTURE_TIME)) as c_hour, count(distinct TERMINAL_MAC) as c_nums from bigdata.terminal_data where COLLECTION_DATE=toDate(now()) group by COLLECTION_EQUIPMENT_MAC,c_hour";
        return sql;
    }
    public String getThisdayTouristSql1(String thisdate) {
        String sql = "select place_id,c_hour,sum(c_nums) as c_nums " +
                " from statistics_tourist a,equipment_data b" +
                " where a.equipment_mac=b.equipment_mac" +
                " and date(c_date)=date('"+thisdate+"')" +
                " group by place_id,c_hour order by place_id,c_hour";
        //String sql = "select COLLECTION_EQUIPMENT_MAC,count(distinct TERMINAL_MAC) as nums from bigdata.terminal_data where COLLECTION_DATE=toDate(now()) GROUP BY COLLECTION_EQUIPMENT_MAC";
        //String sql = "select  COLLECTION_EQUIPMENT_MAC,toHour(toDateTime(CAPTURE_TIME)) as c_hour, count(distinct TERMINAL_MAC) as c_nums from bigdata.terminal_data where COLLECTION_DATE=toDate(now()) group by COLLECTION_EQUIPMENT_MAC,c_hour";
        return sql;
    }
    public String getThisdayAllTouristSql() {
        /*String sql = "select place_id,c_hour,sum(c_nums) as c_nums " +
                " from statistics_tourist a,equipment_data b" +
                " where a.equipment_mac=b.equipment_mac" +
                " and date(c_date)=date('"+thisdate+"')" +
                " group by place_id,c_hour order by place_id,c_hour";*/
        //String sql = "select COLLECTION_EQUIPMENT_MAC,count(distinct TERMINAL_MAC) as nums from bigdata.terminal_data where COLLECTION_DATE=toDate(now()) GROUP BY COLLECTION_EQUIPMENT_MAC";
        String sql = "select  COLLECTION_EQUIPMENT_MAC,count(distinct TERMINAL_MAC) as c_nums from bigdata.terminal_data where COLLECTION_DATE=toDate(now()) group by COLLECTION_EQUIPMENT_MAC";
        return sql;
    }

    public String ecdemicnumSqltype = "clickhouseSearch";
    public String ecdemicnumSql(String mac,String place_type){
        String sql = "";
        if(place_type.equals("3")||place_type.equals("5")||place_type.equals("8")){
            sql="select count(*) as nums from (select distinct(TERMINAL_MAC) from bigdata.terminal_data where COLLECTION_EQUIPMENT_MAC in ("+mac+") and COLLECTION_DATE=toDate(now()) and TERMINAL_MAC not in (select TERMINAL_MAC from bigdata.local_data where COLLECTION_DATE in (select max(COLLECTION_DATE) as COLLECTION_DATE from bigdata.local_data)))";
        }
        else if(place_type.equals("4")){
            sql="select count(*) as nums from (select TERMINAL_MAC,min(CAPTURE_TIME) as first,max(CAPTURE_TIME) as last,last-first as time from bigdata.terminal_data where COLLECTION_EQUIPMENT_MAC in ("+mac+") and COLLECTION_DATE=toDate(now()) and TERMINAL_MAC not in (select distinct TERMINAL_MAC from bigdata.terminal_data where COLLECTION_DATE=(today()-1) and COLLECTION_EQUIPMENT_MAC in ("+mac+")) and TERMINAL_MAC not in (select TERMINAL_MAC from bigdata.local_data where COLLECTION_DATE in (select max(COLLECTION_DATE) as COLLECTION_DATE from bigdata.local_data)) group by TERMINAL_MAC having time>180 and time<43200)";
        }
        else if(place_type.equals("6")){
            sql="select count(*) as nums from (select TERMINAL_MAC,min(CAPTURE_TIME) as first,max(CAPTURE_TIME) as last,last-first as time from bigdata.terminal_data where COLLECTION_EQUIPMENT_MAC in ("+mac+") and COLLECTION_DATE=toDate(now()) and TERMINAL_MAC not in (select distinct TERMINAL_MAC from bigdata.terminal_data where COLLECTION_DATE=(today()-1) and COLLECTION_EQUIPMENT_MAC in ("+mac+")) and TERMINAL_MAC not in (select TERMINAL_MAC from bigdata.local_data where COLLECTION_DATE in (select max(COLLECTION_DATE) as COLLECTION_DATE from bigdata.local_data)) group by TERMINAL_MAC having time<43200)";
        }
        else if(place_type.equals("7")){
            sql="select count(distinct TERMINAL_MAC) as nums from (select TERMINAL_MAC,COLLECTION_EQUIPMENT_MAC,count(*) as nums from bigdata.terminal_data where COLLECTION_DATE=toDate(now()) and TERMINAL_MAC not in (select TERMINAL_MAC from bigdata.local_data where COLLECTION_DATE in (select max(COLLECTION_DATE) as COLLECTION_DATE from bigdata.local_data)) and TERMINAL_MAC not in (select distinct TERMINAL_MAC from bigdata.terminal_data where COLLECTION_DATE=(today()-1) and COLLECTION_EQUIPMENT_MAC in ("+mac+")) and COLLECTION_EQUIPMENT_MAC in ("+mac+") group by TERMINAL_MAC,COLLECTION_EQUIPMENT_MAC having nums>1)";
        }
        else{
            sql="select count(*) as nums from (select TERMINAL_MAC,max(CAPTURE_TIME)-min(CAPTURE_TIME) as time from bigdata.terminal_data where COLLECTION_EQUIPMENT_MAC in ("+mac+") and COLLECTION_DATE=toDate(now()) and TERMINAL_MAC not in (select TERMINAL_MAC from bigdata.local_data where COLLECTION_DATE in (select max(COLLECTION_DATE) as COLLECTION_DATE from bigdata.local_data)) group by TERMINAL_MAC having time>600)";
        }
        return sql;
    }


    public String setYestodayTouristType = "sqliteUpdate";
    public String setYestodayTouristSql(String date,int nums){
        String sql = "insert into persons_data(date,nums) values('"+date+"',"+nums+")";
        return sql;
    }
    public String getYestodayTouristType = "sqliteSearch";
    public String getYestodayTouristSql(){
        String sql = "SELECT equipment_mac from equipment_data as e where e.place_id in (select id as place_id from place_data where place_type not in (5,8,9))";
        return sql;
    }
    public String getThisdayTouristType1 = "clickhouseSearch";
    public String getThisdayTouristSql2(String mac) {
        String sql="select count(*) as nums from (select TERMINAL_MAC,max(CAPTURE_TIME)-min(CAPTURE_TIME) as time from bigdata.terminal_data where  COLLECTION_DATE=(today()-1) and COLLECTION_EQUIPMENT_MAC in ("+mac+") and TERMINAL_MAC not in (select TERMINAL_MAC from bigdata.local_data where COLLECTION_DATE in (select max(COLLECTION_DATE) as COLLECTION_DATE from bigdata.local_data)) group by TERMINAL_MAC having time>7200)";
        return sql;
    }
}
