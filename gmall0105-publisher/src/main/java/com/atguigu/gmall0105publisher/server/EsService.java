package com.atguigu.gmall0105publisher.server;

import java.util.Map;

public interface EsService {
    public Long getDauTotal(String date);

    public Map getDauHour(String date);
}
