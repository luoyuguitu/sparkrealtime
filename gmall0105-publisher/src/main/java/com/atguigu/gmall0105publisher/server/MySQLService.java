package com.atguigu.gmall0105publisher.server;

import java.util.List;
import java.util.Map;

public interface MySQLService {
    //品牌
    public List<Map> getTrademarkStat(String startTime,String endTime,int topN);
}
