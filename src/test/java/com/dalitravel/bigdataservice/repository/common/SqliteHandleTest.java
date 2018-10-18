package com.dalitravel.bigdataservice.repository.common;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SqliteHandleTest {

    @Autowired
    private SqliteDao sh;

    @Test
    public void contextLoads() {
        String str = "select * from area_data";
        //str = "SELECT date('now')";
        //List aa = sh.execSql(str);
        //System.out.print(aa);
    }
}