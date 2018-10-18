package com.dalitravel.bigdataservice.service;

import com.dalitravel.bigdataservice.repository.ExecSqlDao;
import com.dalitravel.bigdataservice.repository.sql.FlowmeterSql;
import com.dalitravel.bigdataservice.repository.sql.StatisticsSql;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Service
public class StatisticsService {
    @Autowired
    private ExecSqlDao ess;
    @Autowired
    private StatisticsSql sSql;
    @Autowired
    private FlowmeterSql fls;
    @Autowired
    private FlowmeterService fs;

    /**
     * 将游客数量进行按时间、wifi机器码进行统计，并保存到sqlite中。
     * @return
     */
    public boolean statisticsTourist() {

        //获取当前已统计数据的最后日期
        List<Map<String, Object>> lastlist = ess.execSql(sSql.getLastdateType, sSql.getLastdateSql());
        String startdate = ((Map) lastlist.get(0)).get("c_date").toString();
        //String startdate = "2018-9-12";
        Calendar starttime = Calendar.getInstance();
        Calendar endtime = Calendar.getInstance();
        int y = endtime.get(Calendar.YEAR);
        int m = endtime.get(Calendar.MONTH);
        int d = endtime.get(Calendar.DATE);
        starttime.set(Integer.parseInt(startdate.split("-")[0]), Integer.parseInt(startdate.split("-")[1]) - 1, Integer.parseInt(startdate.split("-")[2]));
        endtime.set(y, m, d);
        Long startTime = starttime.getTimeInMillis();
        Long endTime = endtime.getTimeInMillis();
        Long oneDay = 1000 * 60 * 60 * 24l;
        Long looptime = startTime + oneDay;//从已统计日期的后一天开始补充统计数据
        if (looptime > endTime + oneDay)
            return true;//如果已经统计到昨天，就直接退出。由于java运行有时间消耗，TimeInMillis的一天间隔一般都大于oneDay。所以把endtime加一天

        endTime = endTime - oneDay;//仅统计到昨天的数据。
        while (looptime < endTime) {
            Date dd = new Date(looptime);
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            statisticsFunc(df.format(dd), df.format(dd));
            looptime += oneDay;
        }
        return true;
    }

    /**
     * 执行语句
     * @return
     */
    public boolean statisticsFunc(String startdate,String enddate){

        /*//判断是否已经存在记录
        List<Map<String, Object>> elist = ess.execSql(sSql.checkStatisticsType,sSql.checkStatisticsSql(startdate,enddate));
        if(Integer.parseInt(((Map)elist.get(0)).get("tn").toString())>0)
            return true;*/
        //根据日期删除数据
        ess.execSql(sSql.delStatisticsType,sSql.delStatisticsSql(startdate,enddate));

        //统计结果保存到表statistics_tourist的sql语句。由于需要拼接sql语句，就直接在此写死，不整合到sql文件中。
        String insertsql = "insert into statistics_tourist(c_year,c_month,c_day,c_date,c_hour,equipment_mac,c_nums) values ";

        String mac = fs.getMacByArea(ess.execSql(fls.getAreaMacType,fls.getAreaMacSql("")));
        List<Map<String, Object>> list = ess.execSql(sSql.getTouristType,sSql.getTouristSql(startdate,enddate,mac));//获取统计数据
        for(Map map:list) {
            insertsql += "(" + (map.get("c_date").toString().split("-"))[0]
                    + "," +(map.get("c_date").toString().split("-"))[1]
                    + ","+(map.get("c_date").toString().split("-"))[2]
                    + ",'" +map.get("c_date").toString() + "'"
                    + ","+map.get("c_hour").toString()
                    + ",'"+map.get("equipment_mac").toString() + "'"
                    + ","+map.get("c_nums").toString()+")"
                    + ",";
        }
        insertsql = insertsql.substring(0,insertsql.length() - 1);
        if(!list.isEmpty()){
            synchronized(this){
                ess.execSql("sqliteUpdate",insertsql);
            }
        }

        return true;
    }
}
