package com.atguigu.gmall0105publisher.server;

import java.math.BigDecimal;
import java.util.Map;

public interface ClickHouseService {
    public BigDecimal getOrderAmount(String date);

    public Map getOrderAmountHour(String date);
}
