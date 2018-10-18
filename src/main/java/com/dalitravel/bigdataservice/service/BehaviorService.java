package com.dalitravel.bigdataservice.service;

import com.dalitravel.bigdataservice.repository.ExecSqlDao;
import com.dalitravel.bigdataservice.repository.sql.BehaviorSql;
import com.dalitravel.bigdataservice.repository.sql.FlowmeterSql;
import io.swagger.models.auth.In;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class BehaviorService {
    @Autowired
    private ExecSqlDao ess;
    @Autowired
    private BehaviorSql bhs;
    @Autowired
    private FlowmeterService fs;
    @Autowired
    private FlowmeterSql fls;

    public List<Map<String, Object>> locus(String startplace){
        List<Map<String, Object>> list = ess.execSql(bhs.getLocusType,bhs.getLocusSql(startplace));
        int allnums = Integer.parseInt(((Map)list.get(0)).get("allnums").toString());
        for(Map map:list){
            int num = Integer.parseInt(map.get("nums").toString());
            map.put("percent",(new DecimalFormat("##.00").format(((double)num*100)/allnums))+"%");
            map.put("trackpath",ess.execSql(bhs.getTrackPathType,bhs.getTrackPathSql(map.get("track_id").toString())));
        }
        return list;
    }

    public void updatelocusnumDate(){

        //获取当前已统计数据的最后日期
        List<Map<String, Object>> lastlist = ess.execSql(bhs.getLastDateLocusNumType, bhs.getLastDateLocusNumSql());
        String startdate = ((Map) lastlist.get(0)).get("c_date").toString();
        Calendar endtime = Calendar.getInstance();
        Calendar starttime = Calendar.getInstance();
        starttime.set(Integer.parseInt(startdate.split("-")[0]), Integer.parseInt(startdate.split("-")[1]) - 1, Integer.parseInt(startdate.split("-")[2]));
        endtime.set(endtime.get(Calendar.YEAR), endtime.get(Calendar.MONTH), endtime.get(Calendar.DATE));
        Long startTime = starttime.getTimeInMillis();
        Long endTime = endtime.getTimeInMillis();
        Long oneDay = 1000 * 60 * 60 * 24l;
        Long looptime = startTime + oneDay;//从已统计日期的后一天开始补充统计数据
        if (looptime > endTime)
            return;//如果已经统计到昨天，就直接退出。

        while (looptime < endTime) {
            Date dd = new Date(looptime);
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            updatelocusnumData(df.format(dd));
            updateStaytime(df.format(dd));
            looptime += oneDay;
        }
    }

    public synchronized void updatelocusnumData(String c_date){

        //删除已经存在记录
        ess.execSql(bhs.delLocusNumType,bhs.delLocusNumSql(c_date));

        //循环获取统计结果
        List<Map<String, Object>> list = ess.execSql(bhs.getAllLocusType,bhs.getAllLocusSql());
        for(Map map:list){
            String startmac = map.get("startmac").toString();
            String endmac = map.get("endmac").toString();
            List<Map<String, Object>> numlist = ess.execSql(bhs.getLocusNumType,bhs.getLocusNumSql(startmac,endmac,c_date));//获取轨迹的游客数量
            if(numlist.size()==0)
                continue;
            int nums = Integer.parseInt(numlist.get(0).get("nums").toString());
            int id = Integer.parseInt(map.get("track_id").toString());
            ess.execSql(bhs.setLocusNumType,bhs.setLocusNumSql(id,nums));//将游客数据更新到最新表中
            ess.execSql(bhs.setLocusNumHisType,bhs.setLocusNumHisSql(id,nums,c_date));//将游客数据更新到历史表中
        }
    }

    public synchronized void updateStaytime(String c_date){
        //删除已经存在记录
        ess.execSql(bhs.delStaytimeType,bhs.delStaytimeSql(c_date));
        //获取所有mac的停留时间，并插入到表statistics_staytime中
        List<Map<String, Object>> list = ess.execSql(bhs.getAllStaytimeType,bhs.getAllStaytimeSql(c_date));
        String insertsql = "";
        for(Map map:list){
            insertsql += "('" + map.get("equipment_mac").toString() + "',"
                    + Integer.parseInt(map.get("staytime").toString())
                    + "," + Integer.parseInt(map.get("staynum").toString())
                    + ",'"+c_date+"'),";
        }
        insertsql = insertsql.substring(0,insertsql.length() - 1);
        ess.execSql(bhs.setAllStaytimeType,bhs.setAllStaytimeSql(insertsql));

        //获取需要显示的停留时间景点清单
        List<Map<String, Object>> list1 = ess.execSql(bhs.getStaytimeType,bhs.getStaytimeSql());
        int alltime = 0;
        for(Map map:list1){
            String place_id = map.get("place_id").toString();
            List<Map<String, Object>> list2 = ess.execSql(bhs.getStaytimebyIdType,bhs.getStaytimebyIdSql(place_id,c_date));
            int staytime = 0;
            for(Map map1:list2){
                staytime+= Integer.parseInt(map1.get("staytime").toString());
            }
            alltime += staytime;
            ess.execSql(bhs.setStaytimeType,bhs.setStaytimeSql(place_id,staytime));
        }
        ess.execSql(bhs.setStaytimeType,bhs.setStaytimeSql("0",alltime));//更新综合数据
    }

    public List<Map<String, Object>> traffic(){
        List<Map<String, Object>> list = ess.execSql(bhs.getTrafficType,bhs.getTrafficSql());
        int allnums = Integer.parseInt(((Map)list.get(0)).get("allnums").toString());
        for(Map map:list){
            int num = Integer.parseInt(map.get("nums").toString());
            map.put("percent",(new DecimalFormat("##.00").format(((double)num*100)/allnums))+"%");
        }
        return list;
    }

    public List<Map<String, Object>> trafficbymonth(String year,String month){
        List<Map<String, Object>> list1 = ess.execSql(bhs.getTrafficByMonthType,bhs.getTrafficByMonthSql(year,month,"'0034CBD49248','0034CBD49279'"));//机场
        List<Map<String, Object>> list2 = ess.execSql(bhs.getTrafficByMonthType,bhs.getTrafficByMonthSql(year,month,"'0034CBD49111','0034CBD490C9','0034CBD48F54'"));//火车站
        List<Map<String, Object>> list3 = ess.execSql(bhs.getTrafficByMonthType,bhs.getTrafficByMonthSql(year,month,"'0034CBD49215','0034CBD4905C','0034CBD48C76'"));//汽车站

        List<Map<String, Object>> list = new ArrayList<Map<String,Object>>();
        Map<String,Object> map1 = new HashMap<String,Object>();
        int num1 = Integer.parseInt(((Map)list1.get(0)).get("nums").toString());
        map1.put("traffic_type","飞机");
        map1.put("nums",num1);
        Map<String,Object> map2 = new HashMap<String,Object>();
        int num2 = Integer.parseInt(((Map)list2.get(0)).get("nums").toString());
        map2.put("traffic_type","火车");
        map2.put("nums",num2);
        Map<String,Object> map3 = new HashMap<String,Object>();
        int num3 = Integer.parseInt(((Map)list3.get(0)).get("nums").toString());
        map3.put("traffic_type","汽车");
        map3.put("nums",num3);
        int allnums = num1+num2+num3;
        map1.put("allnums",allnums);
        map2.put("allnums",allnums);
        map3.put("allnums",allnums);
        list.add(map1);
        list.add(map2);
        list.add(map3);

        for(Map map:list){
            int num = Integer.parseInt(map.get("nums").toString());
            map.put("percent",(new DecimalFormat("##.00").format(((double)num*100)/allnums))+"%");
        }
        return list;
    }

    public List<Map<String, Object>> stay(){
        return ess.execSql(bhs.getStayType,bhs.getStaySql());
    }

    public List<Map<String, Object>> interest(){
        List<Map<String, Object>> list = ess.execSql(bhs.getInterestType,bhs.getInterestSql());
        int allnums = Integer.parseInt(((Map)list.get(0)).get("allnums").toString());
        for(Map map:list){
            int num = Integer.parseInt(map.get("nums").toString());
            map.put("percent",(new DecimalFormat("##.00").format(((double)num*100)/allnums))+"%");
        }
        return list;
    }

    public List<Map<String, Object>> food(){
        return ess.execSql(bhs.getFoodType,bhs.getFoodSql());
    }

    public List<Map<String, Object>> hotel(){
        return ess.execSql(bhs.getHotelType,bhs.getHotelSql());
    }

    /**
     * 获取其各景区的游客数量
     */
    public List<Map<String, Object>> scenicspot(){

        //首先获取景区清单，包括景点名称、景点id
        List<Map<String, Object>> placeList = ess.execSql(bhs.getScenicSpotType,bhs.getScenicSpotSql());

        //获取所有景区mac的游客清单
        List<Map<String, Object>> alltouristlist = fs.getTouristnumByMac(fs.getMacByArea(ess.execSql(fls.getAreaMacType,fls.getAreaMacSql(""))));

        //将景点的游客数据统计到景区清单中
        for(Map map:placeList) {      //其次获取景区下所有位置的mac地址
            List<Map<String, Object>> placemaclist = ess.execSql(bhs.getMacByAreaType, bhs.getMacByAreaSql(map.get("place_id").toString()));
            int tn = 0;
            String mac = "";
            for (Map map1 : placemaclist) {
                mac = map1.get("equipment_mac").toString();
                for (Map map2 : alltouristlist) {
                    if (mac.equals(map2.get("equipment_mac").toString()))
                        tn += Integer.parseInt(map2.get("nums").toString());
                }
            }
            map.put("nums", tn);//组装返回数据，格式为 景点名称、景点id、游客数量。
        }
        Collections.sort(placeList, new Comparator<Map<String, Object>>() {
            public int compare(Map<String, Object> o2, Map<String, Object> o1) {
                Integer name1 = Integer.valueOf(o1.get("nums").toString()) ;//name1是从你list里面拿出来的一个
                Integer name2 = Integer.valueOf(o2.get("nums").toString()) ; //name1是从你list里面拿出来的第二个name
                return name1.compareTo(name2);
            }
        });
        return placeList;
    }


}
