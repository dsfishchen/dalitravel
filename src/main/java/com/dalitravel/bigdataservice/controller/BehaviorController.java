package com.dalitravel.bigdataservice.controller;

import com.dalitravel.bigdataservice.common.vo.Result;
import com.dalitravel.bigdataservice.service.BehaviorService;
import com.dalitravel.bigdataservice.utils.ResultUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@Api(value="行为分析",description="行为分析")
@RequestMapping(value="/behavior")
public class BehaviorController {
    @Autowired
    private BehaviorService bhs;

    @GetMapping(value="/locus")
    @ApiOperation(value="游客轨迹",notes="游客轨迹")
    @ResponseBody
    public Result<Object> locus(@RequestParam String startplace){
        return new ResultUtil<Object>().setData(bhs.locus(startplace));
    }

    @GetMapping(value="/traffic")
    @ApiOperation(value="出行方式",notes="出行方式")
    @ResponseBody
    public Result<Object> traffic(){
        return new ResultUtil<Object>().setData(bhs.traffic());
    }

    @GetMapping(value="/trafficbymonth")
    @ApiOperation(value="出行方式",notes="出行方式")
    @ResponseBody
    public Result<Object> trafficbymonth(@RequestParam String year,@RequestParam String month){
        return new ResultUtil<Object>().setData(bhs.trafficbymonth(year,month));
    }

    @GetMapping(value="/stay")
    @ApiOperation(value="停留时间",notes="停留时间")
    @ResponseBody
    public Result<Object> stay(){
        return new ResultUtil<Object>().setData(bhs.stay());
    }

    @GetMapping(value="/predilection")
    @ApiOperation(value="偏好分析",notes="偏好分析")
    @ResponseBody
    public Result<Object> predilection(@RequestParam String type){
        switch (type){
            case "interest":
                return new ResultUtil<Object>().setData(bhs.interest());
            case "food":
                return new ResultUtil<Object>().setData(bhs.food());
            case "hotel":
                return new ResultUtil<Object>().setData(bhs.hotel());
            case "scenicspot":
                return new ResultUtil<Object>().setData(bhs.scenicspot());
        }
        return new ResultUtil<Object>().setData(bhs.interest());
    }

}
