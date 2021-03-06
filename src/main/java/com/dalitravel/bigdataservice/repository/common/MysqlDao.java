package com.dalitravel.bigdataservice.repository.common;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class MysqlDao {
    @Autowired
    private JdbcTemplate jdbcTemplate;


    public List<Map<String, Object>> execSql(String sql) {
        return jdbcTemplate.queryForList(sql);
    }

}
