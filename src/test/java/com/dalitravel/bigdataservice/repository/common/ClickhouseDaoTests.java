package com.dalitravel.bigdataservice.repository.common;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ClickhouseDaoTests {

    @Autowired
    private ClickhouseDao ch;

    @Test
    public void contextLoads() {
        String str = "select * from bigdata.virtual_data WHERE COLLECTION_YEAR=1996 AND COLLECTION_MONTH=3 AND COLLECTION_DATE='1996-03-01' AND VIRTUAL_TYPE=142";
        //List aa = ch.execSql(str);
        //System.out.print(aa);
    }

}
