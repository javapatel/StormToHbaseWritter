package com.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;


public class ConsumerBolt extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println(tuple.toString());
    }
}
