
import org.apache.thrift.TSerializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import fresto.format.UIEvent;

public class TestPublisher {

	public static void main(String[] args) {
		TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());

		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket publisher = context.socket(ZMQ.PUB);
		publisher.connect("tcp://localhost:7000");

		while(true) {
			UIEvent event = new UIEvent("test", "test", "test", "test", 12345);
			event.setCurrentPlace("testPlace");
			event.setElapsedTime(10);
			//event.setUrl("testurl");
			try {
				System.out.println("Sending...");
				byte[] messageBytes = serializer.serialize(event);
				publisher.send("A".getBytes(), ZMQ.SNDMORE);
				publisher.send(messageBytes, 0);
				System.out.println("Sent");
			} catch(TException te) {
				te.printStackTrace();
			}
			//Thread.sleep(1000);
		}
	}
}

