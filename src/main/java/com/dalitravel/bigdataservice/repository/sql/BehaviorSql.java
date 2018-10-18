package com.dalitravel.bigdataservice.repository.sql;

import org.springframework.stereotype.Service;

@Service
public class BehaviorSql {

    public String getLocusType = "sqliteSearch";
    public String getLocusSql(String startplace){
        switch (startplace){
            case "jichang":
                startplace = "机场";
                break;
            case "huochezhan":
                startplace = "火车站";
                break;
            case "gaosuchukou":
                startplace = "高速路口";
                break;
        }
        String sql = "select id as track_id,start,destination,startmac,endmac,nums,(select sum(nums) from track_data " +
                "where start='"+startplace+"') as allnums from track_data where start='"+startplace+"' order by nums desc limit 0,9";
        return sql;
    }
    public String getAllLocusType = "sqliteSearch";
    public String getAllLocusSql(){
        String sql = "select id as track_id,start,destination,startmac,endmac,nums from track_data";
        return sql;
    }

    //获取已统计的轨迹数据最后日期
    public String getLastDateLocusNumType = "sqliteSearch";
    public String getLastDateLocusNumSql(){
        String sql = "select max(c_date) as c_date from track_data_history";
        return sql;
    }
    //根据日期删除数据
    public String delLocusNumType = "sqliteUpdate";
    public String delLocusNumSql(String c_date){
        String sql = "delete from track_data_history where c_date='"+c_date+"'";
        return sql;
    }

    //根据探针数据获取游客轨迹数据，如从飞机场到大理古城
    public String getLocusNumType = "clickhouseSearch";
    public String getLocusNumSql(String startmac,String endmac,String c_date){
        String sql = " SELECT count(*) AS nums FROM (" +
                " SELECT TERMINAL_MAC,nums1,nums2 FROM " +
                " (" +
                " SELECT TERMINAL_MAC,count(*) AS nums1 FROM bigdata.terminal_data where COLLECTION_DATE=toDate('"+c_date+"') AND COLLECTION_EQUIPMENT_MAC IN (" +
                startmac +
                ") GROUP BY TERMINAL_MAC" +
                " )" +
                " ALL left JOIN" +
                " (" +
                " SELECT TERMINAL_MAC,count(*) AS nums2 FROM bigdata.terminal_data where COLLECTION_DATE=toDate('"+c_date+"') AND COLLECTION_EQUIPMENT_MAC IN (" +
                endmac +
                ") GROUP BY TERMINAL_MAC" +
                " )" +
                " using TERMINAL_MAC " +
                " ) WHERE nums2>0";
        return sql;
    }

    //将前一天的数据更新到track_data表中
    public String setLocusNumType = "sqliteUpdate";
    public String setLocusNumSql(int id,int nums){
        String sql = "update track_data set nums="+nums+" where id="+id;
        return sql;
    }
    public String setLocusNumHisType = "sqliteUpdate";
    public String setLocusNumHisSql(int id,int nums,String c_date){
        String sql = "insert into track_data_history(id,nums,c_date) values ("+id+","+nums+",'"+c_date+"')";
        return sql;
    }

    //处理停留时间更新
    public String delStaytimeType = "sqliteUpdate";
    public String delStaytimeSql(String c_date){
        String sql = "delete from statistics_staytime where c_date='"+c_date+"'";
        return sql;
    }
    public String getStaytimeType = "sqliteSearch";
    public String getStaytimeSql(){
        String sql = "select id,place_name,place_id from stay_data";
        return sql;
    }
    public String getAllStaytimeType = "clickhouseSearch";
    public String getAllStaytimeSql(String c_date){
        String sql = "select equipment_mac,ceil(sum(times/60)/count(*)) AS staytime,count(*) AS staynum" +
                " from (" +
                " select TERMINAL_MAC,COLLECTION_EQUIPMENT_MAC AS equipment_mac,max(CAPTURE_TIME)-min(CAPTURE_TIME) as times" +
                " from bigdata.terminal_data " +
                " where  COLLECTION_DATE=toDate('"+c_date+"')" +
                " and TERMINAL_MAC not in (select TERMINAL_MAC from bigdata.local_data where COLLECTION_DATE in (select max(COLLECTION_DATE) as COLLECTION_DATE from bigdata.local_data)) " +
                " group by COLLECTION_EQUIPMENT_MAC,TERMINAL_MAC" +
                " ) " +
                " GROUP BY equipment_mac";
        return sql;
    }
    public String setAllStaytimeType = "sqliteUpdate";
    public String setAllStaytimeSql(String values){
        String sql = "insert into statistics_staytime(equipment_mac,staytime,staynum,c_date) values "+values;
        return sql;
    }

    public String getStaytimebyIdType = "sqliteSearch";
    public String getStaytimebyIdSql(String place_id,String c_date){
        String sql = " select a.id,a.place_name,sum(c.staytime) as staytime,sum(c.staynum) as staynum,c.c_date" +
                " from place_data a,equipment_data b,statistics_staytime c" +
                " where a.id=b.place_id and b.equipment_mac=c.equipment_mac and a.id="+place_id+" and c.c_date='"+c_date+"'" +
                " group by a.id,a.place_name,c.c_date" +
                " order by c_date";
        return sql;
    }
    public String setStaytimeType = "sqliteUpdate";
    public String setStaytimeSql(String place_id,int time){
        String sql = "update stay_data set time="+time+" where place_id="+place_id;
        return sql;
    }

    public String getTrackPathType = "sqliteSearch";
    public String getTrackPathSql(String track_id){
        String sql = "select track_id,startplace_id,startplace_name,endplace_id,endplace_name,longitude,latitude,pointserial from track_path where track_id="+track_id;
        return sql;
    }

    public String getTrafficType = "sqliteSearch";
    public String getTrafficSql(){
        String sql = "select traffic_type,nums,(select sum(nums) from traffic_data) as allnums from traffic_data order by nums desc";
        return sql;
    }

    public String getTrafficByMonthType = "sqliteSearch";
    public String getTrafficByMonthSql(String year,String month,String macs){
        String sql = "select count(*) as nums from statistics_tourist " +
                "where equipment_mac in ("+macs+") and c_year="+year+" and c_month="+month;
        return sql;
    }

    public String getStayType = "sqliteSearch";
    public String getStaySql(){
        String sql = "select place_name,time from stay_data order by time desc";
        return sql;
    }

    public String getInterestType = "sqliteSearch";
    public String getInterestSql(){
        String sql = "select interest_name,nums,(select sum(nums) from interest_data) as allnums from interest_data order by nums desc limit 0,5";
        return sql;
    }

    public String getFoodType = "mysqlExec";
    public String getFoodSql(){
        String sql = "select name,score,comment from food order by comment desc limit 0,5";
        return sql;
    }

    public String getHotelType = "mysqlExec";
    public String getHotelSql(){
        String sql = "select name,score from hotel where date(datetime) in (select date(max(datetime)) from hotel) order by score desc limit 0,5";
        return sql;
    }

    public String getScenicSpotType = "sqliteSearch";
    public String getScenicSpotSql(){
        String sql = "select place_name,id as place_id from place_data where place_type not in (5,8,9)";
        return sql;
    }

    /**
     * 获取景区的所有mac地址
     */
    public String getMacByAreaType = "sqliteSearch";
    public String getMacByAreaSql(String place_id) {
        String sql = "SELECT equipment_mac,place_id,location from equipment_data as e " +
                "where e.place_id in ('" + place_id + "')";
        return sql;
    }



}
