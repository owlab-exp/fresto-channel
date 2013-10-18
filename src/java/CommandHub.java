import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import java.util.logging.Logger;

public  class CommandHub extends  Thread implements Runnable {
	private static String THIS_CLASS_NAME = "CommandHub";
	private static Logger LOGGER = Logger.getLogger(THIS_CLASS_NAME);

	private static int frontPort = 7008;
	private static int backPort = 7009;
	private ZMQ.Context ctx;

	public  static void main(String[] args) {
		ZMQ.Context context = ZMQ.context(1);

		//ZMQ.Socket frontEnd = context.socket(ZMQ.XSUB);
		ZMQ.Socket frontEnd = context.socket(ZMQ.SUB);
		frontEnd.bind("tcp://*:" + frontPort);

		//ZMQ.Socket backEnd = context.socket(ZMQ.XPUB);
		ZMQ.Socket backEnd = context.socket(ZMQ.PUB);
		backEnd.bind("tcp://*:" + backPort);

		LOGGER.info("Starting Forwarder with " + frontPort + "/" + backPort);

		frontEnd.subscribe("".getBytes());
		
		// Working!
		//ZMQ.proxy(frontEnd, backEnd, null);
		
		// Working!
		ZMQ.device(ZMQ.FORWARDER, frontEnd, backEnd);

		frontEnd.close();
		backEnd.close();
		context.term();
	}
}
