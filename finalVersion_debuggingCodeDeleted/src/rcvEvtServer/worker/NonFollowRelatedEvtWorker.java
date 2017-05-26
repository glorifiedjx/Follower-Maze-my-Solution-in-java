package rcvEvtServer.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import server.RcvEvtServer;
import util.Constants;
import util.LineReader;

/**
 * Events that are nothing to do with 'Follow' will be processed in this Runnable class
 * First, this class reads from data source line by line
 * and then analyze and process non-Follow events ('B', 'P') 
 * Follow related events will be put into followRelatedMap without any analysis. its analysis and process will be done
 * in FollowRelatedEvtWorker class.
 * 
 * @author  glorifiedjx
 * 
 */
public class NonFollowRelatedEvtWorker extends EvtWorker implements Runnable{
	static Logger logger = Logger.getLogger(NonFollowRelatedEvtWorker.class);

	private RcvEvtServer server;
	private Socket socket;
	
	// format: <seqNo, payLoad>
	private TreeMap<Integer, String> broadcastTreeMap;
	// format: <clientId, <seqNo, payLoad>>
	private Map<String, TreeMap<Integer, String>> eventData;

	// messages that are related to 'following' (eg: |S|, |U|, |F| ) should be sorted first. 
	// For that purpose, just accumulate those related messages and pass them
	// format: <seqNo, payloads> 
	private Map<Integer, String> followRelatedMap;
	
	public NonFollowRelatedEvtWorker(Socket socket, RcvEvtServer server) {
		this.server = server;
		this.socket = socket;
		eventData = new ConcurrentHashMap<>();
		followRelatedMap = new ConcurrentHashMap<>();
	}
	
	
	public Map<String, TreeMap<Integer, String>> getEventData() {
		return eventData;
	}
	
	public Map<Integer, String> getFollowRelatedMap() {
		return followRelatedMap;
	}
	
	/**
	 * read each line from data source and parse its
	 */
	@Override
	public void run() {
		BufferedReader br = null;
		LineReader in;
		try {
			br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			in = new LineReader(br);
			String line;
			while((line = in.readLine()) != null) {
				splitEachEvt(line);
			}
		} catch (IOException e) {
			try {
				socket.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			
		} finally {
			server.execute();
		}
	}

	/**
	 * 'B' and 'P' types events are analyzed here. 
	 * For 'B' events, they will all be put into 
	 */
	@Override
	protected void transEvent(String payLoad, int seqNo, String type, String fromId, String toId) {
		if (type.equals("B")) {

			if (broadcastTreeMap == null)	broadcastTreeMap = new TreeMap<>();
			broadcastTreeMap.put(seqNo,payLoad);
			eventData.put(Constants.broad, broadcastTreeMap);
		}
		else if(type.equals("P") && (toId != null) ) {
			if (eventData.get(toId) == null) { 
				TreeMap<Integer, String> tree = new TreeMap<Integer, String>();
				tree.put(seqNo, payLoad);
				eventData.put(toId,tree);
			}
			else
				eventData.get(toId).put(seqNo, payLoad);			
		}
		else {	// any events other than type 'B' and 'P' should be put into 'followRelatedTreeMap'
			followRelatedMap.put(seqNo, payLoad);
		}
		
	}
}
