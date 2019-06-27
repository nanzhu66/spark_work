package com.xzkj.spark_work.control;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import spark_work.SparkWorkers;

@RestController
public class MyControl {

    @RequestMapping(value = "/post", method = RequestMethod.POST)
    public String post_test(@RequestParam String key) {
        String res = new SparkWorkers().work01(key);
        return res;
    }

}
