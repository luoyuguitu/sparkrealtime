package com.atguigu.gmall0105publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0105publisher.server.ClickHouseService;
import com.atguigu.gmall0105publisher.server.EsService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    EsService esService;

    @Autowired
    ClickHouseService clickHouseService;

    @RequestMapping(value = "realtime-total", method = RequestMethod.GET)
    public String realtimeTotal(@RequestParam("date") String date) {
        List<Map<String, Object>> rsList = new ArrayList<>();
        Long dauTotal = esService.getDauTotal(date);

        Map<String, Object> dauMap = new HashMap();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        if (dauTotal != null) {
            dauMap.put("value", dauTotal);
        } else {
            dauMap.put("value", 0L);
        }
        rsList.add(dauMap);

        Map<String, Object> newMidMap = new HashMap();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);
        rsList.add(newMidMap);

        Map<String, Object> orderAmountMap = new HashMap<>();
        orderAmountMap.put("id", "order_amount");
        orderAmountMap.put("name", "新增交易额");
        BigDecimal orderAmount = clickHouseService.getOrderAmount(date);
        orderAmountMap.put("value", orderAmount);
        rsList.add(orderAmountMap);

        return JSON.toJSONString(rsList);
    }

    @GetMapping("realtime-hour")
    public String realtimeHour(@RequestParam("id") String id, @RequestParam("date") String date) {

        //获取昨天日期
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String ydStr;

        try {
            Date todayDate = format.parse(date);
            Date yd = DateUtils.addDays(todayDate, -1);
            ydStr = format.format(yd);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式不正确");
        }

        if ("dau".equals(id)) {
            Map dauHourTD = esService.getDauHour(date);
            Map dauHourYD = esService.getDauHour(ydStr);

            Map<String, Map<String, Long>> rsMap = new HashMap<>();
            rsMap.put("yesterday", dauHourYD);
            rsMap.put("today", dauHourTD);
            return JSON.toJSONString(rsMap);
        } else if ("order_amount".equals(id)) {
            Map orderAmountHourMapTD = clickHouseService.getOrderAmountHour(date);
            Map orderAmountHourMapYD = clickHouseService.getOrderAmountHour(ydStr);

            Map<String,Map<String,BigDecimal>> rsMap=new HashMap<>();
            rsMap.put("yesterday",orderAmountHourMapYD);
            rsMap.put("today",orderAmountHourMapTD);
            return  JSON.toJSONString(rsMap);
        } else
            return null;


    }


}
