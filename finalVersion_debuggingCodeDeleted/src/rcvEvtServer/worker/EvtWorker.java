package rcvEvtServer.worker;

import org.apache.log4j.Logger;

/**
 * This is an abstract Runnable class for 'FollowRelatedEvtWorker' and 'NonFollowRelatedEvtWorker'
 * Every line read from event source will be parsed and translated by inheriting this class
 *
 * @author  glorifiedjx
 * 
 */
public abstract class EvtWorker implements Runnable{
	
	static Logger logger = Logger.getLogger(EvtWorker.class);
	
	/**
	 * Every line read from event source will be parsed and split into payloads' properties.
	 *
	 * @param  payLoad	each line(i.e., payload) received from event source
	 */
	protected void splitEachEvt(String payLoad) {
		// Analyze payload
		String strWithoutLinefeed = payLoad.replace("\n", "").replace("\r", ""); 
		int count =0;
		int seqNo = 0;
		String type = null;
		String fromId = null;
		String toId = null;
		for (String partial: strWithoutLinefeed.split("\\|")) {
			switch (count) {
			case 0:		//Sequence No
				seqNo = Integer.parseInt(partial);
				break;
			case 1:		//Type
				if(partial != null) 	type = partial;
				
				break;
			case 2:		//FromId
				if(partial != null)		fromId = partial;
				break;
			case 3:		//ToId
				if(partial != null)		toId = partial;
				break;
			default:
				logger.debug("!!!should NOT come here!!!!!");
				break;
			}
			count++;
		}
		transEvent(payLoad, seqNo, type, fromId, toId);
	}
	
	/**
	 * process each parsed line(a.k.a payload) in terms of types i.e., (F, U, B, P and S)
	 * 
	 * 'B' and 'P' types are regarded as 'Non-follow related events' meaning the sequence of receiving those events is not 
	 *   really important. 
	 * 'F', 'U', 'S' types are categorized as 'Follow Related events' because its receiving order does matter. 
	 *   For instance, the first event says "A follows B"  and the second event says "A unfollows B". 
	 *   it means nothing follows after all. 
	 *   but in reverse order, ie, "A unfollows B" and then "A follows B" indicates now "A  follows B"
	 * 	 In a word, what matters for 'Follow Related events' is SEQUENCE NUMBER. ie, 'Follow Related events' should be 
	 *   reordered in sequence first to analyze which follows which 
	 *   where as 'Non-follow related events' is not required to be in order. 
	 * 
	 * @param  protocol properties
	 * 
	 */
	protected abstract void transEvent(String payLoad, int seqNo, String type, String fromId, String toId);
}
