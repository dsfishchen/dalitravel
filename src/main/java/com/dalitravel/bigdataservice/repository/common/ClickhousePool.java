package com.dalitravel.bigdataservice.repository.common;

import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.TimerTask;
import java.util.Vector;
import java.util.Timer;

@Service
public class ClickhousePool implements ClickhousePoolInterface {

    private ClickhousePoolProperty dbBean;
    private Boolean isActive = false;
    private Integer contActive = 0; //创建记录总数

    private List<Connection> freeConnections = new Vector<Connection>();    //空闲链接
    private List<Connection> activeConnections = new Vector<Connection>();  //活动连接
    private static ThreadLocal<Connection> threadLocal = new ThreadLocal<Connection>();

    private ClickhousePool(ClickhousePoolProperty dbBean) {
        this.dbBean = dbBean;
        init();
    }

    private static ClickhousePool pool;
    public static ClickhousePool getPool(){
        if(pool == null){
            pool = new ClickhousePool(ClickhousePoolProperty.getBean());
            return pool;
        }else{
            return pool;
        }
    }
    public void init(){
        try{
            Class.forName(dbBean.getDriverName());
            for(int i = 0;i < dbBean.getInitConnection();i++){
                Connection conn = newConnection();
                if(conn!=null){
                    freeConnections.add(conn);
                    contActive++;
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }


    public Connection newConnection() throws ClassNotFoundException, SQLException {
        Connection conn = null;
        if(dbBean != null){
            Class.forName(dbBean.getDriverName());
            conn = DriverManager.getConnection(dbBean.getUrl());
        }
        return conn;
    }

    @Override
    public synchronized Connection getConnection() {
        // TODO Auto-generated method stub
        Connection conn = null;
        try {
            if(contActive < this.dbBean.getMaxActiveConnections()){
                if(freeConnections.size() > 0){
                    conn = freeConnections.get(0);
                    if(conn!=null){
                        threadLocal.set(conn);
                    }
                    freeConnections.remove(0);
                }else{
                    conn = newConnection();
                }
            }else{
                wait(this.dbBean.getConnTimeOut());
                conn = getConnection();
            }
            if(isVaild(conn)){
                activeConnections.add(conn);
                contActive++;
            }
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
        return conn;
    }

    public Boolean isVaild(Connection conn){
        try{
            if(conn == null || conn.isClosed())
            {
                return false;
            }
        }catch(SQLException e){
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public  Connection getCurrentConnection() {
        // TODO Auto-generated method stub
        Connection conn = threadLocal.get();
        if(!isVaild(conn)){
            conn = getConnection();
        }
        return conn;
    }

    @Override
    public synchronized void close(Connection conn) throws SQLException {
        // TODO Auto-generated method stub
        if(isVaild(conn) && !(freeConnections.size()> dbBean.getMaxConnections())){
            freeConnections.add(conn);
            activeConnections.remove(conn);
            contActive--;
            threadLocal.remove();
            notifyAll();
        }
    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub
        for (Connection conn : freeConnections) {
            try {
                if(isVaild(conn))conn.close();
            } catch (Exception e) {
                // TODO: handle exception
                e.printStackTrace();
            }
        }

        for(Connection conn : activeConnections){
            try {
                if(isVaild(conn))conn.close();
            } catch (Exception e) {
                // TODO: handle exception
                e.printStackTrace();
            }
        }

        isActive = false;
        contActive = 0;
    }

    @Override
    public Boolean isActive() {
        // TODO Auto-generated method stub
        return isActive;
    }

    @Override
    public void checkPool() {
        // TODO Auto-generated method stub
        if(dbBean.getIsCheckPool()){
            new Timer().schedule(new TimerTask() {
                public void run() {
                    System.out.println("空闲连接数"+freeConnections.size());
                    System.out.println("活动连接数"+activeConnections.size());
                    System.out.println("总连接数"+contActive);
                }
            }, dbBean.getLazyCheck(),dbBean.getPeriodCheck());
        }

    }
}
