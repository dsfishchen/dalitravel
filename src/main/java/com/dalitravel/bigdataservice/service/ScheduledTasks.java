package com.dalitravel.bigdataservice.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

@Component
@Configuration
@EnableScheduling
public class ScheduledTasks {
    @Autowired
    FlowmeterService fs;
    @Autowired
    BehaviorService bs;
    @Autowired
    StatisticsService ss;
    @Autowired
    JinGuiHotelService js;

    //每五分钟从clickhouse数据库获取游客数量并同步到本地sqlite touristnum_date
    @Scheduled(cron = "0 0/10 *  * * * ")
    public void updateTouristNum(){
        fs.updateTouristNum();
    }

    //每天统计前一天的游客数据，记录到persons_data
    @Scheduled(cron = "0 0 1  * * * ")
    public void updateYestodayTouristNum(){
        fs.updateYestodayTouristNum();
    }

    //每天执行，从clickhouse获取游客轨迹数据、停留时间的数据，同步到sqlite表track_data、track_data_history、statistics_staytime、stay_data
    @Scheduled(cron = "0 0 1  * * * ")
    public void updatelocusnumDate(){
        bs.updatelocusnumDate();
    }

    //每2小时执行，将游客数量进行按时间（每2小时）、wifi机器码进行统计，并保存到sqlite中statistics_tourist。
    //统计当天的数据

    @Scheduled(cron = "0 1 0/2  * * * ")
    public void statisticsThisDayTourist(){
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Calendar time = Calendar.getInstance();
        long timeMillis = time.getTimeInMillis();
        ss.statisticsFunc(df.format(timeMillis),df.format(timeMillis));
    }

    //每天执行，将游客数量进行按时间（每2小时）、wifi机器码进行统计，并保存到sqlite中statistics_tourist。
    @Scheduled(cron = "0 0 1  * * * ")
    public void statisticsTourist(){
        ss.statisticsTourist();
    }

    //每天执行，从金圭获取游客数据，保存到119mysql中
    @Scheduled(cron = "0 0 1  * * * ")
    public void updateCustomerData(){
        js.updateCustomerData();
    }

    //test
    /*
    @Scheduled(cron = "0/3 * * * * * ")
    public void updateCu222stomerData(){
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Calendar time = Calendar.getInstance();
        Long startTime = time.getTimeInMillis();
        System.out.println("开始测试-----------------"+df.format(startTime));
    }
    */
}
