package com.zt.controller;

import com.zt.common.annotation.CheckFunction;
import com.zt.kafka.producter.TestProducter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
@Slf4j
public class TestController {
    @Autowired
    private TestProducter testProducter;

    @RequestMapping(value = "/sendMsg", method = RequestMethod.GET)
    public boolean sendMsg(@RequestParam(value = "msg") String msg) {
        testProducter.sendMsg(msg);
        return true;
    }

    @CheckFunction(value = "check")
    @RequestMapping(value = "/test", method = RequestMethod.GET)
    public Double getMsg(@RequestParam(value = "n") double n, @RequestParam(value = "m") double m) {
        log.info("函数执行中...");
        return n / m;
    }

}
