package server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import fwdEvtServer.worker.NioWorker;


/**
 * FwdEvtServer accepts a multiple number of user clients that are supposed to be notified of events in order
 *  and the rest of work, i.e., receiving ID of user and send them proper events in order etc., 
 *  are delegated to 'NioWorker'.
 *
 * @author  glorifiedjx
 */

public class FwdEvtServer {

	static Logger logger = Logger.getLogger(FwdEvtServer.class);
	
	private InetAddress hostAddress;
	private int port;
	
	private NioWorker nioWorker;
	
	// The channel on which we'll accept connections
	private ServerSocketChannel serverChannel;

	// The selector we'll be monitoring
	private Selector selector;

	// <clientId, <seqNo, payLoad>>
	private Map<String, TreeMap<Integer, String>> eventData;
 
	public FwdEvtServer(InetAddress hostAddress, int port) throws IOException {
		this.hostAddress = hostAddress;
		this.port =  port;
		this.selector = this.initSelector();
		eventData = new ConcurrentHashMap<>();
	}
	
	/**
	 * selector initialization should be done in FwdEvtServer rather than NioWorker thread
	 * so that clients can be connected in the beginning of each round.( each round here means each $ ./followermaze.sh)
	 */ 
	private Selector initSelector() throws IOException {
		Selector socketSelector = SelectorProvider.provider().openSelector();

		this.serverChannel = ServerSocketChannel.open();
		serverChannel.configureBlocking(false);

		InetSocketAddress isa = new InetSocketAddress(hostAddress, port);
		serverChannel.socket().bind(isa);

		serverChannel.register(socketSelector, SelectionKey.OP_ACCEPT);

		return socketSelector;
	}

	public void terminate() {
		if(nioWorker != null)
			nioWorker.setTerminate();
	}
	
	/**
	 * this method starts nioWorker thread. it was found that without this design, creating FwdEvtServer instance first and
	 * let clients wait till ready for sending data AND THEN start NIO thread, it weirdly produces wrong output 
	 * when trying to run it more than once because NIO keeps running in while loop with previous data. 
	 * (here 'run it more than once' means when "$ ./followermaze.sh" is done more than once, meaning different sets of 
	 * data source input coming each round )
	 * So, the NIO algorithm is put into a thread and whenever program runs again, the previous NIO thread should be 
	 * terminated and a new threa should be created for each new round
	 */
	public void execute() {
		nioWorker = new NioWorker(selector, eventData);
		Thread nioThread = new Thread(nioWorker);
		nioThread.start();
	}

	/**
	 * to refresh event data into NIO for each round. 
	 * First it is required to clear eventData, which is important
	 * and put all brand newly analyzed and processed data into eventData 
	 */
	public void refreshEvtData(Map<String, TreeMap<Integer, String>> evtData) {
		eventData.clear();
		eventData.putAll(evtData);
	}
}
