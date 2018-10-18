package com.dalitravel.bigdataservice.controller;


import com.dalitravel.bigdataservice.common.vo.Result;
import com.dalitravel.bigdataservice.service.FlowmeterService;
import com.dalitravel.bigdataservice.utils.ResultUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@Api(value="流量统计",description="流量统计")
@RequestMapping(value="/flowmeter")
public class FlowmeterController {
    @Autowired
    private FlowmeterService fls;

    @GetMapping(value="/num")
    @ApiOperation(value="实时游客人数",notes="实时游客人数")
    @ResponseBody
    public Result<Object> areaTouristNum(@RequestParam String city){
        if(city.equals("all")) //实时游客总人数
            return new ResultUtil<Object>().setData(fls.getAreaTouristNum(""));
        else
            return new ResultUtil<Object>().setData(fls.getPlaceNumByArea(city));
    }

    @GetMapping(value="/spotnum")
    @ApiOperation(value="实时游客人数",notes="实时游客人数")
    @ResponseBody
    public Result<Object> spotnum(@RequestParam String city){
        return new ResultUtil<Object>().setData(fls.getSpotNumByArea(city));
    }

    @GetMapping(value="/spotnum1")
    @ApiOperation(value="实时游客人数",notes="实时游客人数")
    @ResponseBody
    public Result<Object> spotnum1(@RequestParam String city){
        return new ResultUtil<Object>().setData(fls.getSpotNumByArea1(city));
    }

    @GetMapping(value="/ecdemicnum")
    @ApiOperation(value="实时游客人数",notes="实时游客人数")
    @ResponseBody
    public Result<Object> ecdemicnum(@RequestParam String place_id,@RequestParam String place_type){
        return new ResultUtil<Object>().setData(fls.ecdemicnum(place_id,place_type));
    }

    @GetMapping(value="/scale")
    @ApiOperation(value="各县市游客占比",notes="各县市游客占比")
    public Result<Object> scale(){
        return new ResultUtil<Object>().setData(fls.scale());
    }

    @GetMapping(value="/toursitenum")
    @ApiOperation(value="各景点实时游客总人数",notes="各景点实时游客总人数")
    public Result<Object> toursitenum(@RequestParam String city,@RequestParam String place_id){
        return new ResultUtil<Object>().setData(fls.getTouristNumByPlaceid(place_id));
    }

    @GetMapping(value="/toursitepointnum")
    @ApiOperation(value="各景点分区域详细人数",notes="各景点分区域详细人数")
    public Result<Object> toursitepointnum(@RequestParam String city,@RequestParam String place_id){
        return new ResultUtil<Object>().setData(fls.getTouristListByPlaceid(place_id));
    }

    @GetMapping(value="/statistics")
    @ApiOperation(value="各景区按时间统计",notes="各景区按时间统计")
    public Result<Object> statistics(@RequestParam String type,
                                     @RequestParam String place_id,
                                     @RequestParam String year,
                                     @RequestParam String month,
                                     @RequestParam String day){
        return new ResultUtil<Object>().setData(fls.getStatisticsByDate(place_id,type,year,month,day));
    }
}
