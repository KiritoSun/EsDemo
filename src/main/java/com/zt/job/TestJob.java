package com.zt.job;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 定时任务
 */
@Component
public class TestJob {

    /**
     * 定时打印当前时间 每隔5秒执行一次
     */
//    @Scheduled(cron = "0/5 * * * * ?")
//    public void printLog() {
//        Date date = new Date();
//        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
//        String dateStr = formatter.format(date);
//        System.out.println("现在是北京时间：" + dateStr);
//    }

}
