package com.dalitravel.bigdataservice.repository.common;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Service
public class ClickhousePoolProperty {
/*
    @Autowired
    private Environment env;
    private String chhost = env.getProperty("clickhouse.Host");
    private String chport = env.getProperty("clickhouse.Port");
    private String chdb = env.getProperty("clickhouse.db");
    private String chpwd = env.getProperty("clickhouse.Password");
*/
    private String driverName = "ru.yandex.clickhouse.ClickHouseDriver";    //JDBC驱动
    //private String url = "jdbc:clickhouse://" + chhost + ":" + chport + "/" + chdb + "?password=" + chpwd;; //JDBC地址
    private String url = "jdbc:clickhouse://192.168.1.24:8123/bigdata?password=BIGDATAcomedali8896927";

    private String poolName = "clickhousePool";   //连接池名字
    private Integer minConnections = 0; //空闲时最小连接数
    private Integer maxConnections = 10;    //空闲时最大连接数
    private Integer initConnection = 0; //初始化连接数量
    private Long connTimeOut = 1000L;   //重复获得连接的频率
    private Integer maxActiveConnections = 50; //最大允许的连接数
    private Long ConnectionTimeOut = 1000*60*20L;   //连接超时时间
    private Boolean isCurrentConnection = true;//是否获得当前连接
    private Boolean isCheckPool = true;//是否定时检查连接池
    private Long lazyCheck = 1000*60*60L;   //延迟多少时间后开始检查
    private Long periodCheck = 1000*60*60L; //检查频率

    private static ClickhousePoolProperty db;
    private ClickhousePoolProperty(){}//单例模式
    public static ClickhousePoolProperty getBean(){
        if(db == null){
            db = new ClickhousePoolProperty();
            return db;
        }else{
            return db;
        }
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getPoolName() {
        return poolName;
    }

    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }

    public Integer getMinConnections() {
        return minConnections;
    }

    public void setMinConnections(Integer minConnections) {
        this.minConnections = minConnections;
    }

    public Integer getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(Integer maxConnections) {
        this.maxConnections = maxConnections;
    }

    public Integer getInitConnection() {
        return initConnection;
    }

    public void setInitConnection(Integer initConnection) {
        this.initConnection = initConnection;
    }

    public Long getConnTimeOut() {
        return connTimeOut;
    }

    public void setConnTimeOut(Long connTimeOut) {
        this.connTimeOut = connTimeOut;
    }

    public Integer getMaxActiveConnections() {
        return maxActiveConnections;
    }

    public void setMaxActiveConnections(Integer maxActiveConnections) {
        this.maxActiveConnections = maxActiveConnections;
    }

    public Long getConnectionTimeOut() {
        return ConnectionTimeOut;
    }

    public void setConnectionTimeOut(Long connectionTimeOut) {
        ConnectionTimeOut = connectionTimeOut;
    }

    public Boolean getIsCurrentConnection() {
        return isCurrentConnection;
    }

    public void setIsCurrentConnection(Boolean isCurrentConnection) {
        this.isCurrentConnection = isCurrentConnection;
    }

    public Boolean getIsCheckPool() {
        return isCheckPool;
    }

    public void setIsCheckPool(Boolean isCheckPool) {
        this.isCheckPool = isCheckPool;
    }

    public Long getLazyCheck() {
        return lazyCheck;
    }

    public void setLazyCheck(Long lazyCheck) {
        this.lazyCheck = lazyCheck;
    }

    public Long getPeriodCheck() {
        return periodCheck;
    }

    public void setPeriodCheck(Long periodCheck) {
        this.periodCheck = periodCheck;
    }

}
