package com.dalitravel.bigdataservice.controller;

import com.dalitravel.bigdataservice.common.vo.Result;
import com.dalitravel.bigdataservice.service.JinGuiHotelService;
import com.dalitravel.bigdataservice.utils.ResultUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


@RestController
@Api(value="旅游管理",description="旅游管理")
@RequestMapping(value="/jinguihotel")
public class JinGuiHotelController {
    @Autowired
    private JinGuiHotelService jhs;

    @GetMapping(value="/city")
    @ApiOperation(value="各县市旅店数量",notes="各县市旅店数量")
    @ResponseBody
    public Result<Object> city(){
        return new ResultUtil<Object>().setData(jhs.getCityHotelNum());
    }

    @GetMapping(value="/area")
    @ApiOperation(value="各区域旅店数量",notes="各区域旅店数量")
    @ResponseBody
    public Result<Object> area(){
        return new ResultUtil<Object>().setData(jhs.getAreaHotelNum());
    }

    @GetMapping(value="/leavings")
    @ApiOperation(value="各区域剩余房间数、床位数",notes="各区域剩余房间数、床位数")
    @ResponseBody
    public Result<Object> leavings(){
        return new ResultUtil<Object>().setData(jhs.getAreaLeavingHotelNum());
    }

    @GetMapping(value="/onephonetravel")
    @ApiOperation(value="旅店上线一机游的占比情况",notes="旅店上线一机游的占比情况")
    @ResponseBody
    public Result<Object> onephonetravel(){
        return new ResultUtil<Object>().setData(jhs.getOnephonetravelHotelNum());
    }

}
