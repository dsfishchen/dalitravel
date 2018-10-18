package com.dalitravel.bigdataservice.service;


import com.dalitravel.bigdataservice.repository.ExecSqlDao;
import com.dalitravel.bigdataservice.repository.sql.TouristpropertySql;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class TouristpropertyService {

    @Autowired
    private ExecSqlDao ess;
    @Autowired
    private TouristpropertySql tps;

    /**
     * 获取年龄分布
     * @return
     */
    public List<Map<String, Object>> getAgedistribution(){
        return ess.execSql(tps.getAgeDistributionType,tps.getAgeDistributionSql());
    }
    /**
     * 性别分布
     * @return
     */
    public List<Map<String, Object>> getSexDistribution(){
        return ess.execSql(tps.getSexDistributionType,tps.getSexDistributionSql());
    }
    /**
     * 性别分布
     * @return
     */
    public List<Map<String, Object>> getSexofFind(){
        return ess.execSql(tps.getSexofFindType,tps.getSexofFindSql());
    }
    /**
     * 游客来源
     * @return
     */
    public List<Map<String, Object>> getTouristSource(){
        List<Map<String, Object>> list = ess.execSql(tps.getTouristSourceType,tps.getTouristSourceSql());
        int allnums = Integer.parseInt(((Map)list.get(0)).get("allnums").toString());
        for(Map map:list){
            int num = Integer.parseInt(map.get("nums").toString());
            map.put("percent",(new DecimalFormat("0.00").format(((double)num*100)/allnums))+"%");
        }
        return list;
    }
    /**
     * 学历分布
     * @return
     */
    public List<Map<String, Object>> getEducation(){
        List<Map<String, Object>> list = ess.execSql(tps.getEducationType,tps.getEducationSql());
        int allnums = Integer.parseInt(((Map)list.get(0)).get("allnums").toString());
        for(Map map:list){
            int num = Integer.parseInt(map.get("nums").toString());
            map.put("percent",(new DecimalFormat("0.00").format(((double)num*100)/allnums))+"%");
        }
        return list;
    }
}
