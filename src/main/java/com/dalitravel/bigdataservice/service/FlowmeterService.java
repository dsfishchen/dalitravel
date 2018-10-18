package com.dalitravel.bigdataservice.service;

import com.dalitravel.bigdataservice.repository.ExecSqlDao;
import com.dalitravel.bigdataservice.repository.sql.FlowmeterSql;
import io.swagger.models.auth.In;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class FlowmeterService {

    @Autowired
    private ExecSqlDao ess;
    @Autowired
    private FlowmeterSql fls;
    @Autowired
    private StatisticsService sst;

    /**
     * 根据县市名称获取mac地址清单
     */
    public String getMacByArea(List<Map<String, Object>> placemaclist){
        String mac = "";
        for(Map map:placemaclist){
            mac += "'" + map.get("equipment_mac") + "',";
        }
        mac += "'xxxxxxxx'";
        return mac;
    }


    /**
     * 根据mac地址清单获取游客人数
     * @param macstr
     * @return list<map<equipment_mac,nums>>
     */
    public List<Map<String, Object>> getTouristnumByMac(String macstr){
        return ess.execSql(fls.getTouristType,fls.getTouristSql(macstr));
    }

    /**
     * 根据县市获取游客数量
     * @param areatype
     * @return
     */
    public int getAreaTouristNum(String areatype){
        /*
        int areaTouristNum = 0;
        //获取采集设备的mac地址
        String mac = getMacByArea(ess.execSql(fls.getAreaMacType,fls.getAreaMacSql(areatype)));
        //获取游客人数
        List<Map<String, Object>> alltouristlist = getTouristnumByMac(mac);
        for(Map map:alltouristlist){
            areaTouristNum += Integer.parseInt(map.get("nums").toString());
        }*/
        List<Map<String,Object>> list = ess.execSql(fls.getAllTouristType,fls.getAllTouristSql(getMacByArea(ess.execSql(fls.getAreaMacType,fls.getAllAreaMacSql(areatype)))));
        return Integer.parseInt(list.get(0).get("nums").toString());
    }

    /**
     * 获取所有景区的游客数量
     */
    public List<Map<String, Object>> getPlaceNumByArea(String areatype){

        //首先获取景区清单，包括景点名称、景点id
        List<Map<String, Object>> placeList = ess.execSql(fls.getPlaceByAreaType,fls.getPlaceByAreaSql(areatype));
        //List<Map<String, Object>> placeList = ess.execSql(fls.getPlaceByAreaType,fls.getPlaceByAreaSql1(areatype));

        //获取县市下所有景点mac的游客清单
        List<Map<String, Object>> alltouristlist = getTouristnumByMac(getMacByArea(ess.execSql(fls.getAreaMacType,fls.getAreaMacSql(areatype))));
        int allnum = 0;
        for(Map map:alltouristlist){
            allnum += Integer.parseInt(map.get("nums").toString());
        }

        //将景点的游客数据统计到景区清单中
        for(Map map:placeList) {
            int tn = 0;
            String mac = map.get("equipment_mac").toString();
            for (Map map2 : alltouristlist) {
                if (mac.equals(map2.get("equipment_mac").toString()))
                    tn += Integer.parseInt(map2.get("nums").toString());
            }

            map.put("nums", tn);//组装返回数据，格式为 景点名称、景点id、游客数量。
            map.put("allnum", allnum);
            /*
            List<Map<String, Object>> placemaclist = ess.execSql(fls.getMacByPlaceType, fls.getMacByPlaceSql(map.get("place_id").toString()));
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
            map.put("allnum", allnum);*/
        }
        return placeList;
    }


    /**
     * 根据县市获取其各景区的游客数量
     */
    public List<Map<String, Object>> getSpotNumByArea(String areatype){

        //首先获取景区清单，包括景点名称、景点id
        List<Map<String, Object>> placeList = ess.execSql(fls.getPlaceByAreaType,fls.getPlaceByAreaSql1(areatype));

        //获取县市下所有景点mac的游客清单
        List<Map<String, Object>> alltouristlist = getTouristnumByMac(getMacByArea(ess.execSql(fls.getAreaMacType,fls.getAreaMacSql(areatype))));
        int allnum = 0;
        for(Map map:alltouristlist){
            allnum += Integer.parseInt(map.get("nums").toString());
        }

        //提取景点当日按时刻统计的人流数据
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Calendar time = Calendar.getInstance();
        long timeMillis = time.getTimeInMillis();
        List<Map<String,Object>> ttlist = ess.execSql(fls.getThisdayTouristType,fls.getThisdayTouristSql());
        List<Map<String,Object>> ttlist1 = ess.execSql(fls.getThisdayTouristType,fls.getThisdayAllTouristSql());



        //将景点的游客数据统计到景区清单中
        for(Map map:placeList) {
            String place_id = map.get("place_id").toString();
            String equipment_mac = map.get("equipment_mac").toString();
            List<Map<String, Object>> placemaclist = ess.execSql(fls.getMacByPlaceType, fls.getMacByPlaceSql(place_id));
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
            map.put("allnum", allnum);

            List<Map<String,Object>> hourtouristlist = new ArrayList<>();
            tn=0;
            for(Map map3:ttlist){
                //if(map3.get("place_id").toString().equals(place_id)){
                if(map3.get("COLLECTION_EQUIPMENT_MAC").toString().equals(equipment_mac)){
                    int tt = Integer.parseInt(map3.get("c_nums").toString());
                    map3.put("c_nums",tt);
                    hourtouristlist.add(map3);
                    tn += tt;
                }
            }
            for(Map map4:ttlist1){
                if(map4.get("COLLECTION_EQUIPMENT_MAC").toString().equals(equipment_mac)) {
                    tn = Integer.parseInt(map4.get("c_nums").toString());
                }
            }
            map.put("nums", tn);//组装返回数据，格式为 景点名称、景点id、游客数量。
            map.put("hourtouristlist",hourtouristlist);
        }
        return placeList;
    }

    /**
     * 根据县市获取其各景区的游客数量
     */
    public List<Map<String, Object>> getSpotNumByArea1(String areatype){

        //首先获取景区清单，包括景点名称、景点id
        List<Map<String, Object>> placeList = ess.execSql(fls.getPlaceByAreaType,fls.getPlaceByAreaSq2(areatype));

        //获取县市下所有景点mac的游客清单
        List<Map<String, Object>> alltouristlist = getTouristnumByMac(getMacByArea(ess.execSql(fls.getAreaMacType,fls.getAreaMacSql(areatype))));
        int allnum = 0;
        for(Map map:alltouristlist){
            allnum += Integer.parseInt(map.get("nums").toString());
        }

        //提取景点当日按时刻统计的人流数据
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Calendar time = Calendar.getInstance();
        long timeMillis = time.getTimeInMillis();
        List<Map<String,Object>> ttlist = ess.execSql("sqliteSearch",fls.getThisdayTouristSql1(df.format(timeMillis)));

        //将景点的游客数据统计到景区清单中
        for(Map map:placeList) {
            String place_id = map.get("place_id").toString();
            List<Map<String, Object>> placemaclist = ess.execSql(fls.getMacByPlaceType, fls.getMacByPlaceSql(place_id));
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
            map.put("allnum", allnum);

            List<Map<String,Object>> hourtouristlist = new ArrayList<>();
            tn=0;
            for(Map map3:ttlist){
                if(map3.get("place_id").toString().equals(place_id)){
                    int tt = Integer.parseInt(map3.get("c_nums").toString());
                    map3.put("c_nums",tt);
                    hourtouristlist.add(map3);
                    tn += tt;
                }
            }
            map.put("nums", tn);//组装返回数据，格式为 景点名称、景点id、游客数量。
            map.put("hourtouristlist",hourtouristlist);
        }
        return placeList;
    }

    public int ecdemicnum(String place_id,String place_type){
        int nums = 0;
        String mac = getMacByArea(ess.execSql(fls.getMacByPlaceType,fls.getMacByPlaceSql(place_id)));
        List<Map<String,Object>> list = ess.execSql(fls.ecdemicnumSqltype,fls.ecdemicnumSql(mac,place_type));
        if(list.size()<1)
            return 0;
        nums = Integer.parseInt(list.get(0).get("nums").toString());
        if(place_type.equals("5")){
            double basenum=0.16;
            nums = (int)(nums*basenum);
        }
        else if(place_type.equals("8")){
            double basenum=0.75;
            nums = (int)(nums*basenum);
        }
        else if(place_type.equals("3")){
            double basenum;
            if(nums<5000){
                basenum=1;
            }
            else{
                basenum=0.55;
            }
            nums = (int)(nums*basenum);
        }
        else{
        }
        return nums;
    }

    /**
     *各县市游客占比
     */
    public List<Map<String, Object>> scale(){

        List<Map<String,Object>> list = ess.execSql(fls.getAllTouristType,fls.getAllTouristSql(getMacByArea(ess.execSql(fls.getAreaMacType,fls.getAllAreaMacSql("")))));
        int alltouristnum = Integer.parseInt(list.get(0).get("nums").toString());

        //首先获取县市清单，包括县市名称、县市id
        List<Map<String, Object>> placeList = ess.execSql(fls.getAllAreaType,fls.getAllAreaSql());
        for(Map map:placeList) {
            if(map.get("area_type").toString().equals("dali")){
                map.replace("area_id","'"+map.get("area_id")+"'");
                map.replace("area_name","大理市");
                map.replace("longitude","100.232015");
                map.replace("latitude","25.588527");
                break;
            }
        }
        //获取所有县市下所有景点mac的游客清单
        List<Map<String, Object>> alltouristlist = getTouristnumByMac(getMacByArea(ess.execSql(fls.getAreaMacType,fls.getAreaMacSql(""))));
        int allnum = 0;
        for (Map map2 : alltouristlist) {
            allnum += Integer.parseInt(map2.get("nums").toString());
        }

        //将县市的游客数据统计到县市清单中
        for(Map map:placeList) {      //其次获取景点下所有位置的mac地址
            List<Map<String, Object>> placemaclist = ess.execSql(fls.getAreaMacType, fls.getAreaMacSql(map.get("area_type").toString()));
            int tn = 0;
            String mac = "";
            for (Map map1 : placemaclist) {
                mac = map1.get("equipment_mac").toString();
                for (Map map2 : alltouristlist) {
                    if (mac.equals(map2.get("equipment_mac").toString()))
                        tn += Integer.parseInt(map2.get("nums").toString());
                }
            }
            map.put("nums", tn);//组装返回数据，格式为 景点名称、景点id、游客数量、占比。
            map.put("scale", (new DecimalFormat("0.00").format(((double)tn*100)/allnum))+"%");

            if(map.get("area_type").toString().equals("dali")){
                double aa = (double)alltouristnum/(double)allnum;
                tn = (int)(tn*aa);
                map.put("nums", tn);//组装返回数据，格式为 景点名称、景点id、游客数量、占比。
            }
        }
        return placeList;
    }

    /**
     * 获取景点游客数量
     */
    public int getTouristNumByPlaceid(String placeid){
        int placeTouristNum = 0;

        //获取采集设备的mac地址
        String mac = getMacByArea(ess.execSql(fls.getMacByPlaceType,fls.getMacByPlaceSql(placeid)));

        //获取游客人数
        List<Map<String, Object>> alltouristlist = getTouristnumByMac(mac);
        for(Map map:alltouristlist){
            placeTouristNum += Integer.parseInt(map.get("nums").toString());
        }
        return placeTouristNum;
    }


    /**
     * 各景区分景点详细人数
     * @return
     */
    public List<Map<String, Object>> getTouristListByPlaceid(String place_id){
        //首先获取景点清单
        List<Map<String, Object>> placeList = ess.execSql(fls.getMacByPlaceType,fls.getMacByPlaceSql(place_id));

        //获取景点采集设备的mac地址
        String mac = getMacByArea(placeList);

        //获取各景点游客清单
        List<Map<String, Object>> alltouristlist = getTouristnumByMac(mac);

        //将景点的游客数据统计到景区清单中
        for(Map map:placeList) {      //其次获取景点下所有位置的mac地址
            int tn = 0;
            String mac0 = map.get("equipment_mac").toString();
            for (Map map1 : alltouristlist) {
                String mac1 = map1.get("equipment_mac").toString();
                if (mac0.equals(mac1))
                    tn += Integer.parseInt(map1.get("nums").toString());
            }
            map.put("nums", tn);//组装返回数据，格式为 景点名称、景点id、游客数量、占比。
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

    /**
     * 各景区按时间统计
     * @return
     */
    public List<Map<String, Object>> getStatisticsByDate(String place_id,String type,String year,String month,String day){
        String starttime="";
        String endtime="";

        if(month.length()==1) month = "0"+month;

        if(type.equals("day")){/*
            starttime = day;
            endtime = day;*/
            starttime = year+"-"+month+"-01";
            Calendar cal = Calendar.getInstance();
            cal.set(Calendar.YEAR,Integer.parseInt(year));//设置年份
            cal.set(Calendar.MONTH, Integer.parseInt(month)-1);//设置月份
            int lastDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH);//获取某月最大天数
            cal.set(Calendar.DAY_OF_MONTH, lastDay);//设置日历中月份的最大天数
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");//格式化日期
            endtime = sdf.format(cal.getTime());
        }else if(type.equals("month")){
            starttime = year+"-01-01";/*
            Calendar cal = Calendar.getInstance();
            cal.set(Calendar.YEAR,Integer.parseInt(year));//设置年份
            cal.set(Calendar.MONTH, Integer.parseInt(month)-1);//设置月份
            int lastDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH);//获取某月最大天数
            cal.set(Calendar.DAY_OF_MONTH, lastDay);//设置日历中月份的最大天数
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");//格式化日期
            endtime = sdf.format(cal.getTime());*/
            endtime = year+"-12-31";
        }else{//按年统计
            starttime = "2016-01-01";
            endtime = year+"-12-31";
        }

        List<Map<String, Object>> list = ess.execSql(fls.getStatisticsTouristType,fls.getStatisticsTouristSql(type,place_id,starttime,endtime));
        if(place_id.equals("'22','24','5','6','48','23','3','10','12','13','14','17','18','34','35','50','1','2','4','8','9','16','19','20','21','25','7','11','26'")
            || place_id.equals("'3'")){
            List<Map<String, Object>> list1 = ess.execSql(fls.getStatisticsTouristType,fls.getStatisticsTouristSql(type,"",starttime,endtime));
            for(Map map:list){
                for(Map map1:list1){
                    String ctype = "c_"+type;
                    int s = Integer.parseInt(map.get(ctype).toString());
                    int s1 = Integer.parseInt(map1.get(ctype).toString());
                    if(s==s1){
                        int n = Integer.parseInt(map.get("c_nums").toString());//大理市的数据
                        int n1 = Integer.parseInt(map1.get("c_nums").toString());//大理州的数据，要处理，市不能多于州
                        double aa = (double)n1/(double)n;
                        if(aa>1){//州的数据多
                            n = (int)(n/aa);
                        }else//市的数据多
                            n = (int)(n1*aa);
                        map.put("c_nums",n);
                    }
                }
            }
        }
        return list;
    }

    public void updateTouristNum(){
        List<Map<String, Object>> list = ess.execSql(fls.getCKTouristType,fls.getCKTouristSql(""));
        if(list.size()==0)
            return;
        ess.execSql(fls.delTouristType,fls.delTouristSql());
        String insertsql = "";
        for(Map map:list){
            insertsql += "('" + map.get("equipment_mac").toString() + "',"
                    + Integer.parseInt(map.get("nums").toString())
                    + ",2),";
        }
        insertsql = insertsql.substring(0,insertsql.length() - 1);
        ess.execSql(fls.setTouristType,fls.setTouristSql(insertsql));
        ess.execSql(fls.setTouristFlagType,fls.setTouristFlagSql());
        ess.execSql(fls.setLastTouristFlagType,fls.setLastTouristFlagSql());
     }

     public void updateYestodayTouristNum(){
         List<Map<String, Object>> list = ess.execSql(fls.getYestodayTouristType,fls.getYestodayTouristSql());
         String mac = getMacByArea(list);
         List<Map<String, Object>> list1 = ess.execSql(fls.getThisdayTouristType1,fls.getThisdayTouristSql2(mac));
         int nums = Integer.parseInt(list1.get(0).get("nums").toString());

         Date date=new Date();
         Calendar calendar = Calendar.getInstance();
         calendar.setTime(date);
         calendar.add(calendar.DATE, -1);
         date = calendar.getTime();
         SimpleDateFormat format= new SimpleDateFormat("yyyy-MM-dd");
         String dateString = format.format(date);

         ess.execSql(fls.setYestodayTouristType,fls.setYestodayTouristSql(dateString,nums));


     }
}
