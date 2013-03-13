package bolts;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * Populates the cassandra tables.
 * 
 * @author Michael Vogiatzis
 * 
 */
public class CassandraBolt extends BaseRichBolt {
	private static final Logger log = Logger.getLogger(CassandraBolt.class);
	private static Cluster cluster;
	private static Keyspace keyspace;
	private static Mutator<String> mutator;
	OutputCollector ouc;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		ouc = collector;
		// load from properties file
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream("config.properties"));
		} catch (IOException ex) {
			log.error(ex.toString());
		}
		
		//cassandra configuration
		cluster = HFactory.getOrCreateCluster(
				prop.getProperty("CLUSTERNAME"), prop.getProperty("HOST"));
		ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
		ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
		keyspace = HFactory.createKeyspace("links", cluster, ccl,
				FailoverPolicy.FAIL_FAST);
		mutator = HFactory.createMutator(keyspace, StringSerializer.get());
	}

	@Override
	public void execute(Tuple tuple) {
		String expUrl = tuple.getStringByField("expUrl");

		if (expUrl != null) {
			String shortUrl = tuple.getStringByField("shortUrl");
			updateDB(shortUrl, expUrl);
			log.info("DB Update!");
		}

		ouc.ack(tuple);
	}

	
	/**
	 * Updates the cassandra table links with unshortened urls.
	 * @param shortUrl
	 * @param expandedUrl
	 */
	public void updateDB(String shortUrl, String expandedUrl){
		mutator.addInsertion(shortUrl, "l", HFactory.createColumn("u", expandedUrl));
		mutator.execute();
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
