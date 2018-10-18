package com.dalitravel.bigdataservice.repository.common;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MysqlHandleTest {
    @Autowired
    private MysqlDao mh;

    @Test
    public void exeSql() {
        String sql = "select * from airport_bus";
        List ls = mh.execSql(sql);
        System.out.print(ls);
    }
}