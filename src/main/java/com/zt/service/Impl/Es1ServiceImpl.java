package com.zt.service.Impl;

import com.zt.common.annotation.ES;
import com.zt.service.EsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@ES("ES1")
public class Es1ServiceImpl implements EsService {
    @Override
    public boolean save(Object obj) {
        log.info("使用Es1存储对象：{}", obj);
        return true;
    }
}
