package com.dalitravel.bigdataservice.service;

import com.dalitravel.bigdataservice.BigdataserviceApplicationTests;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.*;

public class BehaviorServiceTest extends BigdataserviceApplicationTests {
    @Autowired
    BehaviorService bs;

    @Test
    public void ttt(){
        //bs.updatelocusnumData("2018-08-12");
        bs.updatelocusnumData("2018-10-15");
        bs.updateStaytime("2018-10-15");
    }

}