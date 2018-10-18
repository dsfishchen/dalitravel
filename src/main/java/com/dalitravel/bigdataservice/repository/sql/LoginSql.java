package com.dalitravel.bigdataservice.repository.sql;

import org.springframework.stereotype.Service;

@Service
public class LoginSql {

    public String getLoginType = "sqliteSearch";
    public String getLoginSql(String username,String passwd) {
        String sql = "select * from user_data where user_name='"+username+"' and user_password='"+passwd+"'";
        return sql;
    }



}
