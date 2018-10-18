package com.dalitravel.bigdataservice.controller;

import com.dalitravel.bigdataservice.BigdataserviceApplicationTests;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Calendar;

import static org.junit.Assert.*;

public class FlowmeterControllerTest extends BigdataserviceApplicationTests {

    @Autowired
    FlowmeterController fc;

    @Test
    public void alltourist() {
        long start,end;
        start = System.currentTimeMillis();
        //System.out.println(fc.areaTouristNum("all"));
        System.out.println(fc.areaTouristNum("all"));
        end = System.currentTimeMillis();
        System.out.println("start time:" + start+ "; end time:" + end+ "; Run Time:" + (end - start) + "(ms)");
    }

    @Test
    public void scale() {
        long start,end;
        start = System.currentTimeMillis();
        //System.out.println(fc.areaTouristNum("all"));
        System.out.println(fc.scale());
        end = System.currentTimeMillis();
        System.out.println("start time:" + start+ "; end time:" + end+ "; Run Time:" + (end - start) + "(ms)");
    }
    @Test
    public void toursitenum() {
        long start,end;
        start = System.currentTimeMillis();
        //System.out.println(fc.areaTouristNum("all"));
        System.out.println(fc.toursitenum("","1"));
        end = System.currentTimeMillis();
        System.out.println("start time:" + start+ "; end time:" + end+ "; Run Time:" + (end - start) + "(ms)");
    }
    @Test
    public void toursitepointnum() {
        long start,end;
        start = System.currentTimeMillis();
        //System.out.println(fc.areaTouristNum("all"));
        System.out.println(fc.toursitepointnum("","3"));
        end = System.currentTimeMillis();
        System.out.println("start time:" + start+ "; end time:" + end+ "; Run Time:" + (end - start) + "(ms)");
    }
    @Test
    public void statistics() {
/*
        String startdate = "2018-9-12";
        String enddate = "2018-9-13";
        Calendar starttime=Calendar.getInstance();
        Calendar endtime=Calendar.getInstance();
        starttime.set(Integer.parseInt(startdate.split("-")[0]),Integer.parseInt(startdate.split("-")[1])-1,Integer.parseInt(startdate.split("-")[2]));
        endtime.set(Integer.parseInt(enddate.split("-")[0]),Integer.parseInt(enddate.split("-")[1])-1,Integer.parseInt(enddate.split("-")[2]));
        Long st = starttime.getTimeInMillis();
        Long et = endtime.getTimeInMillis();
        Long oneDay = 1000 * 60 * 60 * 24l;
        String results = "false";
        if(et==st+oneDay)   results = "true";
        System.out.println(et-st);
        System.out.println(starttime);
        System.out.println(endtime);
        System.out.println(results);
*/

        long start,end;
        start = System.currentTimeMillis();
        //System.out.println(fc.areaTouristNum("all"));
        System.out.println(fc.statistics("year","3","2017","12","2018-9-10"));
        end = System.currentTimeMillis();
        System.out.println("start time:" + start+ "; end time:" + end+ "; Run Time:" + (end - start) + "(ms)");

    }

    @Test
    public void spotnum(){
        fc.spotnum("dali");
    }

}