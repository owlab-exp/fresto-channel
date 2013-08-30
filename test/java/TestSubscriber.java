
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import fresto.format.UIEvent;

public class TestSubscriber {

	public static void main(String[] args) {
		TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());

		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
		subscriber.connect("tcp://fresto1.owlab.com:7001");
		subscriber.subscribe("A".getBytes());
		subscriber.subscribe("U".getBytes());

		while(true) {
			System.out.println("Waiting...");
			String envelope = new String(subscriber.recv(0));
			byte[] messageBytes = subscriber.recv(0);
			try {
				UIEvent event = new UIEvent();
				deserializer.deserialize(event, messageBytes);
				System.out.println("Message Envelope: " + envelope);
			       System.out.println("Event.stage : " + event.getStage());
			       System.out.println("Event.clientId : " + event.getClientId());
			       System.out.println("Event.currentPage : " + event.getCurrentPage());
			       System.out.println("Event.uuid : " + event.getUuid());
			       System.out.println("Event.url : " + event.getUrl());
			       System.out.println("Event.timestamp : " + event.getTimestamp());
			} catch(TException te) {
				te.printStackTrace();
			}
		}
	}
}

