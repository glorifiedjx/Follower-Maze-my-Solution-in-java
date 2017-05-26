package fwdEvtServer.worker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import rcvEvtServer.worker.FollowRelatedEvtWorker;
import util.Constants;

/**
 * This is a main Nonblocking I/O Runnable class that deals with 
 *   accepting a multiple number of clients and send proper event data in order to clients. 		
 * 
 * @author  glorifiedjx
 * 
 */
public class NioWorker implements Runnable{
	
	static Logger logger = Logger.getLogger(NioWorker.class);
	
	private volatile boolean isAlive = true;
	private ExecutorService pool;
	private Queue<SocketChannel> toWrite;
	private Selector selector;

	// <socketChannel, clientId> - corresponding socketChannels to connected client ids
	private Map<SocketChannel, String> pendingData;
	// <clientId, <seqNo, payLoad>>
	private Map<String, TreeMap<Integer, String>> eventData;

	
	public NioWorker(Selector selector, Map<String, TreeMap<Integer, String>> eventData) {
		pool = Executors.newFixedThreadPool(100);
		pendingData = new ConcurrentHashMap<>();
		toWrite = new ConcurrentLinkedQueue<>();
		this.eventData =eventData;
		this.selector = selector;
	}

	/**
	 * main algorithm for Non-blocking I/O
	 */
	@Override
	public void run() {
		while(isAlive) {
			try {
				// Wait for an event one of the registered channels
				selector.select();

				SocketChannel changeToWrite;
				while((changeToWrite = toWrite.poll()) != null) {
					changeToWrite.register(selector, SelectionKey.OP_WRITE);
				}

				// Iterate over the set of keys for which events are available
				Iterator selectedKeys = this.selector.selectedKeys().iterator();
				while(selectedKeys.hasNext()) {
					SelectionKey key = (SelectionKey) selectedKeys.next();
					selectedKeys.remove();

					if(!key.isValid())	continue;

					// Check what event is available and deal with it
					if (key.isAcceptable()) {
						accept(key);
					} else if(key.isReadable()) {
						read(key);
					} else if (key.isWritable()) {
						write(key);
					}
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}		
		
	}
	
	/**
	 * write method. For those connected clients that have at least one 'F', 'P' type events to receive, 
	 * they will simply get their events from eventData looking at its id as a key but for those that 
	 * have no id in eventData needs to get only 'B' type messages. 
	 *
	 * @param  key	SelectionKey 
	 */
	private void write(SelectionKey key) throws IOException, InterruptedException {

		SocketChannel sc = (SocketChannel) key.channel();
		String ccId = pendingData.get(sc);
		StringBuffer combinePayloads = new StringBuffer();
		if(eventData.get(ccId.trim()) != null) {
			for(Entry<Integer, String> entry : eventData.get(ccId.trim()).entrySet()) {
				combinePayloads.append(entry.getValue());
			}
		} 
		else {
			if (eventData.get(Constants.broad) != null) {
				for(Entry<Integer, String> entry : eventData.get(Constants.broad).entrySet()) {
					combinePayloads.append(entry.getValue());
				}
			}
		}

		if (combinePayloads.length() > 0) {	 
			sc.write(ByteBuffer.wrap(combinePayloads.toString().getBytes("UTF-8")));
		} 
		sc.register(key.selector(), SelectionKey.OP_READ);
	}

	/**
	 * read method. note that 100 threads are assigned to each reading
	 * check client id here
	 *
	 * @param  key	SelectionKey 
	 */
	private void read(SelectionKey key)  {
		SocketChannel sc = (SocketChannel) key.channel();
		ByteBuffer buf = ByteBuffer.allocateDirect(16); 	// bytes size for each reading ID from connected clients eg) 2932\r\n
		int read = -1;
		try {
			read = sc.read(buf);
		} catch (IOException e1) {}

		if (read == -1) {
			pendingData.remove(sc);
			return;
		}
		pool.submit(new Runnable() {
			@Override
			public void run() {
				buf.flip();
				String ccId = bb_to_str(buf, Charset.forName("UTF-8"));
				pendingData.put(sc, ccId); 	// socketChannel as a key, id of a connected client as a value

				Selector selector = key.selector();
				toWrite.add(sc);
				selector.wakeup();
			}
		});
	}

	/**
	 * accept method
	 *
	 * @param  key	SelectionKey 
	 */
	private void accept(SelectionKey key) throws IOException {
		ServerSocketChannel serverSocketChannel  = (ServerSocketChannel) key.channel();

		SocketChannel socketChannel = serverSocketChannel.accept();
		socketChannel.configureBlocking(false);
		socketChannel.register(key.selector(), SelectionKey.OP_READ);
	}

	/**
	 * convert ByteBuffer to String
	 *
	 * @param  buffer	bytebuffer
	 * @param  buffer	charset
	 */
	private String bb_to_str(ByteBuffer buffer, Charset charset){
		byte[] bytes;
		if(buffer.hasArray()) {
			bytes = buffer.array();
		} else {
			bytes = new byte[buffer.remaining()];
			buffer.get(bytes);
		}
		return new String(bytes, charset);
	}

	/**
	 * to stop this thread. Once user tries to run this program again, a previous running NIO thread should be stopped.  
	 */
	public void setTerminate() {
		isAlive = false;
	}
	
	/**
	 * All analyzed event data from RcvEvtServer will be put into eventData here
	 *
	 * @param  evtData	analyzed event Data from RcvEvtServer
	 */
	public void fillEvtData(Map<String, TreeMap<Integer, String>> evtData) {
		eventData.putAll(evtData);
	}
}
