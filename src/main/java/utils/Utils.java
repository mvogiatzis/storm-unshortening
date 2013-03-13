package utils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;


public class Utils {
	
	private static final Logger log = Logger.getLogger(Utils.class);
	private static JsonFactory f = new JsonFactory();
	
	/**
	 * Calls a third-party URL unshortening service and returns the resolved URL, if successful. 
	 * Otherwise, it returns null.
	 * 	JSON Format
	 *	{
	 *	   "requestedURL":"http:\/\/tinyurl.com\/cn3m36",
	 *	   "success":"true",
	 *	   "resolvedURL":"http:\/\/www.flickr.com\/photos\/ladigue_99\/304431482\/"
	 *	}
	 * @param requestedUrl The url to be resolved
	 * @return The resolved URL if successful.
	 */
	public static String unshortenIt(String requestedUrl){
		
		String expandedURL = null;
		try{
		URL url = new URL("http://api.unshort.me/?r=" + requestedUrl + "&t=json");
		//open URL connection
		URLConnection conn = url.openConnection();
		
		//unshort.me returns an XML if invalid url is passed as an input
		JsonParser jp = f.createJsonParser(conn.getInputStream());
		
		jp.nextToken(); // will return JsonToken.START_OBJECT (verify?)
		jp.nextToken(); // will place token to fieldname 'resolvedURL:'
		jp.nextToken(); // will place token to value of resolvedURL
		
		expandedURL = jp.getText();// get value of resolvedURL
		jp.nextToken(); // place token to 'success' fieldname
		jp.nextToken(); //place token to value of success
		String attempt = jp.getText();
		//if cannot be expanded return the original URL
		if (attempt.equals("false"))
			return null;
		}
		catch(JsonParseException jpe){
			//unshort.me returns XML format in case of invalid URL, and it happens often.
			return null;
		} catch (MalformedURLException e) {
			log.error(e.toString());
		} catch (IOException e) {
			//usually Http Response code 503
			log.error(e.toString());
		}

		return expandedURL;
	}
	
	
	
	
	

	
}
