
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import fresto.event.HttpRequestEvent;
import fresto.event.HttpResponseEvent;

public class TestHttpSubscriber {

	public static void main(String[] args) {
		TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());

		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
		subscriber.connect("tcp://fresto1.owlab.com:7003");
<<<<<<< HEAD
		subscriber.subscribe("".getBytes());

		while(true) {
			System.out.println("Waiting...");
			String envelope = new String(subscriber.recv(0));
			byte[] messageBytes = subscriber.recv(0);
			System.out.println("Received...");
			try {
			       HttpRequestEvent event = new HttpRequestEvent();
			       deserializer.deserialize(event, messageBytes);
			       System.out.println("Message Envelope: " + envelope);
			       System.out.println("Event.httpMethod : " + event.getHttpMethod());
			       System.out.println("Event.localHost : " + event.getLocalHost());
			       System.out.println("Event.localPort : " + event.getLocalPort());
			       System.out.println("Event.contextPath : " + event.getContextPath());
			       System.out.println("Event.servletPath : " + event.getServletPath());
			       System.out.println("Event.frestoUuid : " + event.getFrestoUUID());
			       System.out.println("Event.timestamp : " + event.getTimestamp());
			} catch(TException te) {
				te.printStackTrace();
=======
		subscriber.subscribe("HB".getBytes());
		subscriber.subscribe("HE".getBytes());

		while(true) {
			System.out.println("Waiting...");
			String topic = new String(subscriber.recv(0));
			if("HB".equals(topic)) {
				byte[] messageBytes = subscriber.recv(0);
				System.out.println(messageBytes.length + " bytes received.");
				try {
				       HttpRequestEvent event = new HttpRequestEvent();
				       deserializer.deserialize(event, messageBytes);
				       System.out.println("Message Topic:\t" + topic);
				       System.out.println("Event.httpMethod:\t" + event.getHttpMethod());
				       System.out.println("Event.localHost:\t" + event.getLocalHost());
				       System.out.println("Event.localPort:\t" + event.getLocalPort());
				       System.out.println("Event.contextPath:\t" + event.getContextPath());
				       System.out.println("Event.servletPath:\t" + event.getServletPath());
				       System.out.println("Event.frestoUuid:\t" + event.getFrestoUUID());
				       System.out.println("Event.typeName:\t" + event.getTypeName());
				       System.out.println("Event.signatureName:\t" + event.getSignatureName());
				       System.out.println("Event.depth:\t" + event.getDepth());
				       System.out.println("Event.timestamp:\t" + event.getTimestamp());
				} catch(TException te) {
					te.printStackTrace();
				}
			}
			if("HE".equals(topic)) {
				byte[] messageBytes = subscriber.recv(0);
				System.out.println(messageBytes.length + " bytes received.");
				try {
				       HttpResponseEvent event = new HttpResponseEvent();
				       deserializer.deserialize(event, messageBytes);
				       System.out.println("Message Topic:\t" + topic);
				       System.out.println("Event.responseCode:\t" + event.getResponseCode());
				       System.out.println("Event.frestoUuid:\t" + event.getFrestoUUID());
				       System.out.println("Event.typeName:\t" + event.getTypeName());
				       System.out.println("Event.signatureName:\t" + event.getSignatureName());
				       System.out.println("Event.depth:\t" + event.getDepth());
				       System.out.println("Event.timestamp:\t" + event.getTimestamp());
				} catch(TException te) {
					te.printStackTrace();
				}
>>>>>>> develop
			}
		}
	}
}

