package com.dalitravel.bigdataservice.service;

import com.dalitravel.bigdataservice.repository.ExecSqlDao;
import com.dalitravel.bigdataservice.repository.sql.BehaviorSql;
import com.dalitravel.bigdataservice.repository.sql.FlowmeterSql;
import com.dalitravel.bigdataservice.repository.sql.LoginSql;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Service
public class LoginService {
    @Autowired
    private ExecSqlDao ess;
    @Autowired
    private LoginSql ls;

    public List<Map<String, Object>> logincheck(String username,String passwd){
        List<Map<String, Object>> list = ess.execSql(ls.getLoginType,ls.getLoginSql(username,passwd));

        return list;
    }

}
