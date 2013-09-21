import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import java.util.logging.Logger;

public  class UIEventStreamer extends  Thread implements Runnable {
	private static Logger LOGGER = Logger.getLogger("UIEventStreamer");
	private static int frontPort = 7001;
	private static int backPort = 7004;
	private ZMQ.Context ctx;

	public  static void main(String[] args) {
		ZMQ.Context context = ZMQ.context(1);

		//ZMQ.Socket frontEnd = context.socket(ZMQ.XSUB);
		ZMQ.Socket frontEnd = context.socket(ZMQ.SUB);
		frontEnd.connect("tcp://*:" + frontPort);
		frontEnd.subscribe("".getBytes());

		//ZMQ.Socket backEnd = context.socket(ZMQ.XPUB);
		ZMQ.Socket backEnd = context.socket(ZMQ.PUSH);
		backEnd.bind("tcp://*:" + backPort);

		LOGGER.info("Starting Streamer with " + frontPort + "/" + backPort);

		
		// Working!
		//ZMQ.proxy(frontEnd, backEnd, null);
		
		// Working!
		ZMQ.device(ZMQ.STREAMER, frontEnd, backEnd);

		frontEnd.close();
		backEnd.close();
		context.term();
	}
}
