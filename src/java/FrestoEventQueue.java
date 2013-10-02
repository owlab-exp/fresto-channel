import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Iterator;

import java.util.logging.Logger;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class FrestoEventQueue extends Thread {
	Logger LOGGER = Logger.getLogger("FrestoEventQueue");

	private ConcurrentLinkedQueue<FrestoEvent> queue = new ConcurrentLinkedQueue<FrestoEvent>();
	private AtomicBoolean work = new AtomicBoolean(true);
	private ZMQ.Socket receiveSocket;

	public FrestoEventQueue(ZMQ.Socket receiveSocket) {
		this.receiveSocket = receiveSocket;
	}
	public void run() {
		while(work.get()) {
			String topic = new String(receiveSocket.recv(0)); 
			byte[] eventBytes = receiveSocket.recv(0);
			FrestoEvent frestoEvent = new FrestoEvent(topic, eventBytes);
			queue.add(frestoEvent);
			//LOGGER.fine("EventQueue size = " + queue.size());
		}
	}

	public void stopWork() {
		this.work.set(false);
	}

	public int size() {
		return queue.size();
	}
	
	public FrestoEvent poll() {
		return this.queue.poll();
	}

	public Iterator<FrestoEvent> getIterator() {
		return queue.iterator();
	}
	
	public void remove(FrestoEvent event) {
		this.queue.remove(event);
	}
}


	
