package com.dalitravel.bigdataservice.service;

import com.dalitravel.bigdataservice.BigdataserviceApplicationTests;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.*;

public class FlowmeterServiceTest extends BigdataserviceApplicationTests {

    @Autowired
    FlowmeterService fs;

    @Test
    public void updateTouristNum() {
        fs.updateTouristNum();
    }

    @Test
    public void updateYestodayTouristNum() {
        fs.updateYestodayTouristNum();
    }

}