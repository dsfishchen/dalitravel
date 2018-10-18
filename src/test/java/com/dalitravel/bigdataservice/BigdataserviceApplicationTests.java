package com.dalitravel.bigdataservice;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

@RunWith(SpringRunner.class)
@SpringBootTest
//由于是Web项目，Junit需要模拟ServletContext，因此我们需要给我们的测试类加上@WebAppConfiguration。
@WebAppConfiguration
public class BigdataserviceApplicationTests{

    protected Long startTime;
    protected Long endTime;
    protected DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Before
    public void init() {
        Calendar time = Calendar.getInstance();
        startTime = time.getTimeInMillis();
        System.out.println("开始测试-----------------"+df.format(startTime));
    }

    @After
    public void after() {
        Calendar time = Calendar.getInstance();
        endTime = time.getTimeInMillis();
        System.out.println("测试结束-----------------"+df.format(endTime));
        System.out.println("持续时间-----------------"+(endTime-startTime)/1000+"秒");
    }

    @Test
    public void tt(){
        int nums = 85488;
        int a=80040;
        int b=106436;
        double aa = (double)a/(double)b;
        nums = (int)(nums*aa);
        double basenum=0.16;
        nums = (int)(nums*basenum);
        System.out.println(nums);

    }

}
