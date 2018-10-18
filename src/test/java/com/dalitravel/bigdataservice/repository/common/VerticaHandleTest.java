package com.dalitravel.bigdataservice.repository.common;

import com.dalitravel.bigdataservice.BigdataserviceApplicationTests;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class VerticaHandleTest extends BigdataserviceApplicationTests {

    @Autowired
    private VerticaDao vh;

    @Test
    public void execSql() {

        String sql = "select * from wifi_data where week(insert_time)=week(now()-13)";
        List ls = vh.execSql(sql);
        System.out.print(ls);
    }
}