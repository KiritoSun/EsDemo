package com.zt.controller;

import com.zt.kafka.producter.TestProducter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class TestController {
    @Autowired
    private TestProducter testProducter;

    @RequestMapping(value = "/sendMsg", method = RequestMethod.GET)
    public boolean sendMsg(@RequestParam(value = "msg") String msg) {
        testProducter.sendMsg(msg);
        return true;
    }

}
