package topology;

import spouts.TwitterSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.CassandraBolt;
import bolts.UnshortenBolt;

/**
 * Orchestrates the elements and forms a topology to run the unshortening service.
 * 
 * @author Michael Vogiatzis
 */
public class UnshortenTopology {
	
	public static void main (String[] args) throws Exception{
	TopologyBuilder builder = new TopologyBuilder();
    
    builder.setSpout("spout", new TwitterSpout(), 1);
    
    builder.setBolt("unshortenBolt", new UnshortenBolt(), 8)
             .shuffleGrouping("spout");
    builder.setBolt("dbBolt", new CassandraBolt(), 5)
             .shuffleGrouping("unshortenBolt").setDebug(true);

    Config conf = new Config();
    conf.setDebug(false);

    
    if(args!=null && args.length > 0) {
        conf.setNumWorkers(3);
        
        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    } else {        
        conf.setMaxTaskParallelism(10);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("unshortening", conf, builder.createTopology());
    
        Thread.sleep(30000);

        cluster.shutdown();
    }
	}
}
