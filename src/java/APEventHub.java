import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public  class APEventHub extends  Thread implements Runnable {
	private static int frontPort = 7002;
	private static int backPort = 7003;
	private ZMQ.Context ctx;
	private ZMQ.Socket front;
	private ZMQ.Socket back;

	public  static void main(String[] args) {
		ZMQ.Context context = ZMQ.context(1);

		//ZMQ.Socket frontEnd = context.socket(ZMQ.XSUB);
		ZMQ.Socket frontEnd = context.socket(ZMQ.SUB);
		frontEnd.bind("tcp://*:" + frontPort);

		//ZMQ.Socket backEnd = context.socket(ZMQ.XPUB);
		ZMQ.Socket backEnd = context.socket(ZMQ.PUB);
		backEnd.bind("tcp://*:" + backPort);

		System.out.println("Starting Forwarder with " + frontPort + "/" + backPort);

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
