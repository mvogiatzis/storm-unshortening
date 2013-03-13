package bolts;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import java.net.*;

import org.apache.log4j.Logger;

import spouts.TwitterSpout;
import twitter4j.Status;
import twitter4j.URLEntity;
import utils.Utils;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Gets a Tweet status and emits shortened urls along with their expansion if any.
 * 
 * @author Michael Vogiatzis
 *
 */
public class UnshortenBolt extends BaseRichBolt{
	private static final Logger log = Logger.getLogger(UnshortenBolt.class);
	private OutputCollector ouc;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		ouc = collector;
	}
	
	@Override
	public void execute(Tuple tuple) {
		Status s = (Status) tuple.getValueByField("tweet");
		URLEntity[] urls = s.getURLEntities();
		for (URLEntity url : urls){
			String shortURL = url.getURL();
			String expandedURL = Utils.unshortenIt(shortURL);
			if (expandedURL!=null)
				{ouc.emit(new Values(shortURL, expandedURL));
				log.info("Emitting: " + shortURL + ", "+expandedURL);
				}
		}
		
		ouc.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("shortUrl", "expUrl"));
	}





}
