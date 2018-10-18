package com.dalitravel.bigdataservice.service;

import com.dalitravel.bigdataservice.BigdataserviceApplicationTests;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Calendar;

import static org.junit.Assert.*;

public class JinGuiHotelServiceTest extends BigdataserviceApplicationTests {

    @Autowired
    JinGuiHotelService jgs;

    @Test
    public void getSexDistribution() {

        Calendar time = Calendar.getInstance();
        int hour = time.get(Calendar.HOUR_OF_DAY);
        int mintue = time.get(Calendar.MINUTE);


        //jgs.getSexDistribution();
        //getSextotal 按性别    *      getProviencetotal   按省份    *      getPtTotal  按年龄
        jgs.updateCustomerData();

        //System.out.println(Integer.parseInt(""));

    }
    @Test
    public void getProviceDistribution() {

        Calendar time = Calendar.getInstance();
        int hour = time.get(Calendar.HOUR_OF_DAY);
        int mintue = time.get(Calendar.MINUTE);


        //jgs.getSexDistribution();
        //getSextotal 按性别    *      getProviencetotal   按省份    *      getPtTotal  按年龄
        System.out.println(jgs.getTouristAge());

        //System.out.println(Integer.parseInt(""));

    }
}