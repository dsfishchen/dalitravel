package com.dalitravel.bigdataservice.common.vo;

import lombok.Data;

import java.io.Serializable;

/**
 * @Title: Result
 * @Description:
 * @Auther: Libra
 * @email: 331980830@qq.com
 * @Date: 2018/9/4
 * @version: 1.0.0
 */
@Data
public class Result<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 成功标志
     */
    private boolean success;

    /**
     * 失败消息
     */
    private String message;

    /**
     * 返回代码
     */
    private Integer code;

    /**
     * 时间戳
     */
    private long timestamp = System.currentTimeMillis();

    /**
     * 结果对象
     */
    private T result;
}
