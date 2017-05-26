package main;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import server.FwdEvtServer;
import server.RcvEvtServer;
//import server.Server;

/**
 * This is a main class that runs for "back-end developer Challenge: Follow Maze"
 * It is supposed to create an instance of 'FwdEvtServer' first and then 'RcvEvtServer'.
 * 'FwdEvtServer' is to accept a single event source that sends a stream of events and 
 * 'RcvEvtServer' is to accept many clients that wait for notifications for events. 
 * It is also supposed that execute() method is called after creation of RcvEvtServer instance.
 * Note that FwdEvtServer also has an execute() method but it should not be invoked in Main class.
 *
 * JVM version 1.7 or higher recommended 
 * 
 * @author  glorifiedjx
 * 
 */
public class Main {
	
	static Logger logger = Logger.getLogger(Main.class);
	
	private static final int rcvEvtPort = 9090;
	private static final int fwdEvtport = 9099;
	private static final InetAddress addr = null;

	public static void main(String[] args) {
		DOMConfigurator.configure("log4j.xml");
		FwdEvtServer fwdServer = null;
		try {
			fwdServer = new FwdEvtServer(addr, fwdEvtport);
			RcvEvtServer rcvServer = new RcvEvtServer(addr, rcvEvtPort, fwdServer);
			rcvServer.execute();
			
		} catch(IOException e) {
			logger.info("App already running...exiting");
		} 
	}
}
