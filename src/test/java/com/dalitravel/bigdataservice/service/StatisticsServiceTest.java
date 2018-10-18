package com.dalitravel.bigdataservice.service;

import com.dalitravel.bigdataservice.BigdataserviceApplicationTests;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static org.junit.Assert.*;

public class StatisticsServiceTest extends BigdataserviceApplicationTests {

    @Autowired
    FlowmeterService fs;
    @Autowired
    BehaviorService bs;
    @Autowired
    StatisticsService ss;
    @Autowired
    JinGuiHotelService js;
    @Test
    public void statisticsTourist() {

        ss.statisticsTourist();
    }

    @Test
    public void mytest() {

        double d = 25*100;
        double a = 123;
        double aa = d/a;

        String ss1 = (new DecimalFormat("0.00").format(((double)77914*100)/91663))+"%";
        System.out.println(ss1);
        ss.statisticsFunc("2018-10-10","2018-10-10");
    }

    @Test
    public void statisticsToudddrist() {
        fs.updateTouristNum();
        bs.updatelocusnumDate();
        ss.statisticsTourist();
        js.updateCustomerData();
    }
}