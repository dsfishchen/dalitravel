package com.dalitravel.bigdataservice.controller;


import com.dalitravel.bigdataservice.common.vo.Result;
import com.dalitravel.bigdataservice.service.FlowmeterService;
import com.dalitravel.bigdataservice.service.LoginService;
import com.dalitravel.bigdataservice.utils.ResultUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.lang.reflect.Method;

@RestController
@Api(value="登录",description="登录")
@RequestMapping(value="/login",method = RequestMethod.POST)
public class LoginController {
    @Autowired
    private LoginService ls;

    @GetMapping(value="/login")
    @ApiOperation(value="登录",notes="登录")
    @ResponseBody
    public Result<Object> areaTouristNum(@RequestParam String username,@RequestParam String passwd){
        return new ResultUtil<Object>().setData(ls.logincheck(username,passwd));
    }
}
