package rcvEvtServer.worker;

import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import util.Constants;

/**
 * Those events related with 'Follow' process, i.e., type S, U and F are dealt in 'ProcessOrderedEvtWorker' thread		
 * 
 * @author  glorifiedjx
 * 
 * Note that i may delete all debugging code later. Just leaving them for reference
 */
public class FollowRelatedEvtWorker extends EvtWorker implements Runnable{

	static Logger logger = Logger.getLogger(FollowRelatedEvtWorker.class);

	private NonFollowRelatedEvtWorker nonFollowRelatedEvtWorker;

	// <clientId, <seqNo, payLoad>>
	private Map<String, TreeMap<Integer, String>> eventData;
	// <seqNo, payloads>
	private Map<Integer, String> followRelatedMap;

	// <(fromUserId when 'S' | toUserId when 'F'&'U') , <followers, <seqNo, payloads>>
	private Map<String,  ConcurrentHashMap<String,ConcurrentHashMap<Integer, String>>> followers;  

	public FollowRelatedEvtWorker(NonFollowRelatedEvtWorker nonFollowRelatedEvtWorker, 
			Map<String, TreeMap<Integer, String>> eventData) {
		this.nonFollowRelatedEvtWorker = nonFollowRelatedEvtWorker;
		this.eventData = eventData;
		followRelatedMap = new ConcurrentHashMap<>();
		followers = new ConcurrentHashMap<>();
	}

	/**
	 * main algorithm for FollowRelatedEvtWorker	 
	 * it first waits till nonFollowRelatedEvtWorker's work ever begins. Once it began, keep receiving event data from 
	 * nonFollowRelatedEvtWorker while comparing size of event Data from nonFollowRelatedEvtWorker.
	 * Once all event data is received, analyze 'Follow' related events and process them.
	 */
	@Override
	public void run() {
		boolean omg = false;
		while(!omg) {
			try {
				if (nonFollowRelatedEvtWorker.getEventData().size() > 0) {
					break;
				}
			} catch(NullPointerException e) {
				logger.debug("in catch");	
			}
		}
		int eventDataSize = 0, followSize = 0;
		int extSize, extFollowSize = 0;
		while(true) {
			try {
				Thread.sleep(1000);		// ever one second, update size
				extSize =  nonFollowRelatedEvtWorker.getEventData().size();
				extFollowSize = nonFollowRelatedEvtWorker.getFollowRelatedMap().size();
				if( (eventDataSize < extSize)){
					logger.debug("less than external eventData Size");	
					eventDataSize=extSize;
					continue;
				} else if(followSize < extFollowSize)  {
					logger.debug("less than external follow Size  innerFollowSize: " + followSize + "  extFSize: " + extFollowSize);	
					followSize = extFollowSize;
					continue;
				}
				else if( (eventDataSize == extSize) && (followSize == extFollowSize) ) {
					logger.debug("same");	
					eventData.putAll(nonFollowRelatedEvtWorker.getEventData());
					followRelatedMap.putAll(nonFollowRelatedEvtWorker.getFollowRelatedMap());

					logger.debug("internal evtData Size: "+eventDataSize +" exteal eventData Size: " + extSize);
					logger.debug("internal followSize Size: "+followRelatedMap.size() +" external eventData Size: " + extFollowSize);

					analyzeAndProcessFollowRelatedEvts();
					
					break;
				} else 
					continue;

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Follow Related events will be dealt in here. 
	 * First, follow related events should be parsed line by line and then analyze them,
	 * and all the analyzed data will be put into eventData map.
	 * Finally,'B' type events will also be added into each eventData. ( its key is a client id) 
	 */
	private void analyzeAndProcessFollowRelatedEvts() {
		processFollowRelatedEvts();
		followRelatedEvtsIntoEventData();

		// once eventData is filled in, the last step is to put all 'B' type events into each client
		for (Entry<String, TreeMap<Integer, String>> eventDataEntry : eventData.entrySet()) {
			if(!eventDataEntry.getKey().trim().equals(Constants.broad)) {
				eventDataEntry.getValue().putAll(eventData.get(Constants.broad));
			}
		}
	}

	/**
	 *  all follow related events, which are  analyzed in transEvent method, will also be put into EventData map
	 */
	private void followRelatedEvtsIntoEventData() {
		if(followers != null) {
			for (Entry<String,  ConcurrentHashMap<String,ConcurrentHashMap<Integer, String>>> entry : followers.entrySet()) {
				ConcurrentHashMap<String,ConcurrentHashMap<Integer, String>> eachFollower= entry.getValue();
				for(Entry<String, ConcurrentHashMap<Integer, String>> entryOfEachFollower : eachFollower.entrySet()) {
					TreeMap<Integer, String> payload = (eventData.get(entryOfEachFollower.getKey()) == null) ? 
							new TreeMap<>() : eventData.get(entryOfEachFollower.getKey());

							payload.putAll(entryOfEachFollower.getValue());
							eventData.put(entryOfEachFollower.getKey(), payload);
				}
			}
		}

	}

	/**
	 * events other than non-follow related events, i.e., all follow related events (type F, S, U) will be 
	 *  parsed here line by line.
	 */
	private void processFollowRelatedEvts() {
		//		logger.debug("in doFollowingProcess(),  followSize Size: "+followRelatedMap.size()); 
		for(Entry<Integer, String> entry: followRelatedMap.entrySet()) {	
			String payLoad =entry.getValue();
			splitEachEvt(payLoad);
		}
	}


	/**
	 * parsed events (type F, S, U) are analyzed here. 
	 */
	@Override
	protected void transEvent(String payLoad, int seqNo, String type, String fromId, String toId) {
		if (type.equals("U")) {
			// do nothing if no follower available
			if (followers.get(toId) == null || followers.get(toId).size() < 1)	return;
			// there maybe at least a follower but no message to pass to eventData available
			if (followers.get(toId).get(fromId) != null && followers.get(toId).get(fromId).size() > 0) {
				ConcurrentHashMap<Integer, String> theUnfollowedPayLoad = followers.get(toId).get(fromId);
				if (eventData.get(fromId) == null) {
					TreeMap<Integer, String> tree = new TreeMap<Integer, String>();
					tree.putAll(theUnfollowedPayLoad);
					eventData.put(fromId, tree);
				} else {
					eventData.get(fromId).putAll(theUnfollowedPayLoad);
				}
			}
			followers.get(toId).remove(fromId);
		}
		else if( type.equals("F") ) {
			if (eventData.get(toId) == null) { 
				TreeMap<Integer, String> tree = new TreeMap<Integer, String>();
				tree.put(seqNo, payLoad);
				eventData.put(toId,tree);
			}
			else
				eventData.get(toId).put(seqNo, payLoad);


			ConcurrentHashMap<Integer, String> newPayload = new ConcurrentHashMap<>();	// create empty newPayload
			if(followers.get(toId) != null && followers.get(toId).size() > 0 ) {		// at least one follower exists
				if(followers.get(toId).get(fromId) == null) 	// but this very follower now isn't there so we need to add it
					followers.get(toId).put(fromId, newPayload);
			} else {		// no follower exists so simply add a new follower to
				ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>> newFollower= new ConcurrentHashMap<>();
				newFollower.put(fromId, newPayload);
				followers.put(toId, newFollower);
			}
		} 
		else if(type.equals("S") && fromId != null) {
			if (followers.get(fromId) != null) {
				for(Entry<String, ConcurrentHashMap<Integer, String>> eachFollower : followers.get(fromId).entrySet()) {
					followers.get(fromId).get(eachFollower.getKey()).put(seqNo, payLoad);
				}
			}
		}		
	}
}
