package com.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;

public class StormHbaseLauncher {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException {

        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("word")
                .withColumnFields(new Fields("word"))
                .withCounterFields(new Fields("count"))
                .withColumnFamily("cf");

        HBaseBolt hbaseBolt= new HBaseBolt("WordCount", mapper);



        final BrokerHosts zkrHosts = new ZkHosts("localhost:2181");
        final String kafkaTopic = "sample.request";
        final String zkRoot = "D:\\tmp\\kafka-logs\\";
        final String clientId = "storm2";
        final SpoutConfig kafkaConf = new SpoutConfig(zkrHosts, kafkaTopic, zkRoot, clientId);
        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        // Build topology to consume message from kafka and print them on console
        KafkaSpout spout = new KafkaSpout(kafkaConf);


        // wordSpout ==> countBolt ==> HBaseBolt
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("Kafka_Spout", spout, 1);
        builder.setBolt("Hbase_Bolt", hbaseBolt, 1).fieldsGrouping("Hbase_Bolt", new Fields("word"));


        Config config=new Config();
        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, builder.createTopology());
            Thread.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
            System.exit(0);
        } else {
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
    }
}