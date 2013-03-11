package spouts;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TwitterSpout extends BaseRichSpout{

	private static final Logger log = Logger.getLogger(TwitterSpout.class);
	
	SpoutOutputCollector _collector;
    LinkedBlockingQueue<Status> queue = null;
    
	@Override
	public void open(Map confMap, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		queue = new LinkedBlockingQueue<Status>(1000);
		
		//implement a listener for twitter statuses
		StatusListener listener = new StatusListener(){
	        public void onStatus(Status status) {
	            queue.offer(status);
	        }
	        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
	        public void onException(Exception ex) {
	            ex.printStackTrace();
	        }
			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
			}
			@Override
			public void onStallWarning(StallWarning warning) {
			}
	    };
	    
	    //twitter stream authentication setup
	    ConfigurationBuilder twitterConf = new ConfigurationBuilder();
	    twitterConf.setIncludeEntitiesEnabled(true);
	    //TODO read from properties file
	    twitterConf.setOAuthAccessToken("890618534-kB2aktHhcM90Zuro0XIjAHrGPHdbj5wAXnpJk9x3");
	    twitterConf.setOAuthAccessTokenSecret("pV1HUx4mSLix0e8xREGlzuIXHpulcbj5zu0N5c4jA");
	    twitterConf.setOAuthConsumerKey("IktUV9pWPfmzTbwbQTNQMQ");
	    twitterConf.setOAuthConsumerSecret("MjQM7iATW4G1Co42eeV0AdysrGz605wC8uK4mkAUZvw");
	    TwitterStream twitterStream = new TwitterStreamFactory(twitterConf.build()).getInstance();
	    twitterStream.addListener(listener);
	    
	    // sample() method internally creates a thread which manipulates TwitterStream and calls 
	    //the listener methods continuously.
	    twitterStream.sample();
	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		Utils.sleep(500);
        if(ret==null) {
        	//if queue is empty sleep the spout thread so it doesn't consume resources
            Utils.sleep(50);
        } else {
            _collector.emit(new Values(ret));
            log.info(ret.getUser().getName() + " : " + ret.getText());
        }
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
	
	

}
