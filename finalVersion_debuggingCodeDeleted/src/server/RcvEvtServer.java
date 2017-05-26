package server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import rcvEvtServer.worker.NonFollowRelatedEvtWorker;
import rcvEvtServer.worker.FollowRelatedEvtWorker;

/**
 * This is a RcvEvtServer class
 * This class basically shows every logic for 'Follower Maze' task in execute() method. Steps are described below:
 * 1. accepts a single event source that sends a stream of events in blocking mode
 * 2. 'ProcessUnOrderedEvtWorker' is created to read the stream of events line by line. 
 * 	    It also translates events nothing to do with 'Follow' process i.e., payload with type 'B' and 'P', which are entitled 'UNORDERED EVENTS'
 * 3. Those events related with 'Follow' process, a.k.a 'ORDERED EVENTS' are now dealt in 'ProcessOrderedEvtWorker' thread		
 * 4. By the end of 'ProcessOrderedEvtWorker' process, the 'eventData' map may obtain all required information for events. i.e., to whom payload should be sent etc.
 * 	    It is , therefore, time to create a thread, 'NioWorker' to send those data to clients by invoking fwdEvtServer.execute()
 * 
 * @author  glorifiedjx
 * 
 */
public class RcvEvtServer {
	static Logger logger = Logger.getLogger(RcvEvtServer.class);

	private FwdEvtServer fwdEvtServer;
	
	private ServerSocket ss= null;
	
	// form: Map<recvCliID, TreeMap<seqNo, payLoad>>
	private Map<String, TreeMap<Integer, String>> eventData;

	public RcvEvtServer(InetAddress hostAddress, int port, FwdEvtServer fwdEvtServer) throws IOException {
		ss = new ServerSocket(port);
		this.fwdEvtServer = fwdEvtServer;
		eventData = new ConcurrentHashMap<>();
		logger.info("event source 서버가 생성되었습니다. ^^");

	}
	
	/**
	 * This is a RcvEvtServer class
	 * It is a main algorithm in this program. 
	 * 1. accept data source 
	 * 2. previous Nio Thread should be dead now and eventData should be cleared to prepare this new round.(initializeB4Executing())
	 * 3. NonFollowRelatedEvtWorker starts so that non follow related events can be dealt with
	 * 4. FollowRelatedEvtWorker deals with follow related events
	 * 5. Right after all events are dealt with, it is fwdEvtServer's job to send those received events to clients by starting NioWorker. 
	 */
	
	public void execute() {
		while(true) {
			Socket s;
			try {
				s = ss.accept();
				String reip = s.getInetAddress().getHostAddress();
				logger.info("Server Log1: " + reip);
				
				initializeB4Executing();

				NonFollowRelatedEvtWorker unOrderedEvtWorker = new NonFollowRelatedEvtWorker(s, this);
				Thread unorderedThread = new Thread(unOrderedEvtWorker);
				unorderedThread.start();

				FollowRelatedEvtWorker orderedEvtWorker = new FollowRelatedEvtWorker(unOrderedEvtWorker, eventData);
				Thread orderedThread = new Thread(orderedEvtWorker);
				orderedThread.start();
				
				orderedThread.join();
				fwdEvtServer.refreshEvtData(eventData);
				fwdEvtServer.execute();

			} catch (IOException ex) {
				ex.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}	
	}

	/**
	 * to terminate Nioworker thread and clear eventData up. 
	 * */
	private void initializeB4Executing() {
		fwdEvtServer.terminate();
		eventData.clear();
	}
}
