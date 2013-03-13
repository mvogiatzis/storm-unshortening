package cassandra.schema;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * Database schema builds automatically if the keyspaces do not exist.
 * 
 * @author Michael Vogiatzis
 * 
 */
public class Schema {

	private static final String TWEETS = "tw";
	private static final String LINKS = "links";

	private static final String SIMPLESTRATEGY = "org.apache.cassandra.locator.SimpleStrategy";

	private static int replFactor;
	static String host;
	static String clusterName;
	static Cluster cluster;

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Properties prop = new Properties();
		replFactor = 1;
		try {
			prop.load(Schema.class.getClassLoader().getResourceAsStream("config.properties"));
			replFactor = Integer.valueOf(prop.getProperty("REPL_FACTOR"));
			host = String.valueOf(prop.getProperty("HOST"));
			clusterName = String.valueOf(prop.getProperty("CLUSTERNAME"));
			System.out.println("Replication factor: " + replFactor);
			System.out.println("Host: " + host);
			System.out.println("Cluster name: " + clusterName);

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		cluster = HFactory.getOrCreateCluster(clusterName, host);

		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(TWEETS);

		if (keyspaceDef == null) {
			createTweetsTable();
		}
		
		keyspaceDef = cluster.describeKeyspace(LINKS);

		if (keyspaceDef == null) {
			createLinksTable();
		}
		
	}

	private static void createTweetsTable() {

		final String QF_ID = "id";

		// Column Family tweets
		final String CF_TWEET = "t";
		final String QF_TEXT = "txt";

		ColumnFamilyDefinition cfTweet = HFactory.createColumnFamilyDefinition(
				TWEETS, CF_TWEET, ComparatorType.UTF8TYPE);
		cfTweet.setKeyValidationClass(ComparatorType.LONGTYPE.getTypeName());

		List<ColumnDefinition> tweetMetaData = new ArrayList<ColumnDefinition>();

		BasicColumnDefinition t_txt = new BasicColumnDefinition();
		t_txt.setName(StringSerializer.get().toByteBuffer(QF_TEXT));
		t_txt.setValidationClass(ComparatorType.UTF8TYPE.getClassName());
		tweetMetaData.add(t_txt);
		cfTweet.addColumnDefinition(t_txt);

		List<ColumnFamilyDefinition> cfDefs = new ArrayList<ColumnFamilyDefinition>();
		cfDefs.add(cfTweet);

		KeyspaceDefinition tweetsDef = HFactory.createKeyspaceDefinition(
				TWEETS, SIMPLESTRATEGY, replFactor, cfDefs);

		cluster.addKeyspace(tweetsDef);
	}

	private static void createLinksTable() {
		// column family l
		final String CF_LINKS = "l";
		final String QF_URL = "u";

		ColumnFamilyDefinition cfLinks = HFactory.createColumnFamilyDefinition(
				LINKS, CF_LINKS, ComparatorType.UTF8TYPE);
		cfLinks.setKeyValidationClass(ComparatorType.UTF8TYPE.getTypeName());

		List<ColumnDefinition> linksMetaData = new ArrayList<ColumnDefinition>();

		BasicColumnDefinition twLinks = new BasicColumnDefinition();
		twLinks.setName(StringSerializer.get().toByteBuffer(QF_URL));
		twLinks.setValidationClass(ComparatorType.UTF8TYPE.getClassName());
		linksMetaData.add(twLinks);
		cfLinks.addColumnDefinition(twLinks);

		List<ColumnFamilyDefinition> cfDefs = new ArrayList<ColumnFamilyDefinition>();
		cfDefs.add(cfLinks);

		KeyspaceDefinition linksDef = HFactory.createKeyspaceDefinition(LINKS,
				SIMPLESTRATEGY, replFactor, cfDefs);

		cluster.addKeyspace(linksDef);
	}

}
