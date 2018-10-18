package com.dalitravel.bigdataservice.controller;

import com.dalitravel.bigdataservice.common.vo.Result;
import com.dalitravel.bigdataservice.service.JinGuiHotelService;
import com.dalitravel.bigdataservice.service.TouristpropertyService;
import com.dalitravel.bigdataservice.utils.ResultUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@Api(value="游客属性",description="游客属性")
@RequestMapping(value="/touristproperty")
public class TouristpropertyController {

    @Autowired
    private TouristpropertyService tps;
    @Autowired
    private JinGuiHotelService jgs;

    @GetMapping(value="/agedistribution")
    @ApiOperation(value="年龄结构-进入大理",notes="年龄结构-进入大理")
    @ResponseBody
    public Result<Object> agedistribution(){
        return new ResultUtil<Object>().setData(jgs.getTouristAge());
    }

    @GetMapping(value="/sexdistribution")
    @ApiOperation(value="性别比例-进入大理",notes="性别比例-进入大理")
    @ResponseBody
    public Result<Object> sexdistribution(){
        return new ResultUtil<Object>().setData(jgs.getSexDistribution());
    }

    @GetMapping(value="/sexoffind")
    @ApiOperation(value="搜索大理-性别",notes="搜索大理-性别")
    @ResponseBody
    public Result<Object> sexoffind(){
        return new ResultUtil<Object>().setData(tps.getSexofFind());
    }

    @GetMapping(value="/ageofsearch")
    @ApiOperation(value="搜索大理-年龄",notes="搜索大理-年龄")
    @ResponseBody
    public Result<Object> ageofsearch(){
        return new ResultUtil<Object>().setData(tps.getAgedistribution());
    }

    @GetMapping(value="/touristsource")
    @ApiOperation(value="游客来源省份-进入大理",notes="游客来源省份-进入大理")
    @ResponseBody
    public Result<Object> touristsource(){
        //return new ResultUtil<Object>().setData(jgs.getTouristProvice());//取从金圭获取的实时数据
        return new ResultUtil<Object>().setData(tps.getTouristSource());//取写死的采样数据
    }

    @GetMapping(value="/touristcity")
    @ApiOperation(value="游客来源城市",notes="游客来源城市")
    @ResponseBody
    public Result<Object> touristcity(){
        return new ResultUtil<Object>().setData(jgs.getTouristCity());
    }

    @GetMapping(value="/education")
    @ApiOperation(value="学历分布",notes="学历分布")
    @ResponseBody
    public Result<Object> education(){
        return new ResultUtil<Object>().setData(tps.getEducation());
    }

}
