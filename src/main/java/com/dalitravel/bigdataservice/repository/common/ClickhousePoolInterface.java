package com.dalitravel.bigdataservice.repository.common;

import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.SQLException;

@Service
public interface ClickhousePoolInterface {
    public Connection getConnection();  //获得连接
    public Connection getCurrentConnection();   //获得当前连接
    public void close(Connection conn) throws SQLException; //回收连接
    public void destroy();  //销毁连接池
    public Boolean isActive();  //获取连接池状态
    public void checkPool();    //检查连接池状态
}
