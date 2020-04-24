package com.zt.common.context;

import com.google.common.collect.Maps;
import com.zt.service.EsService;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class EsContext {

    private Map<String, EsService> map = Maps.newConcurrentMap();

    public void put(String name, EsService service) {
        map.put(name, service);
    }

    public EsService get(String name) {
        return map.get(name);
    }

}
