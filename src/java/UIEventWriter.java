import java.util.logging.Logger;
import java.io.IOException;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailStructure;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;

import fresto.event.HttpRequestEvent;
import fresto.event.HttpResponseEvent;

import fresto.data.FrestoData;
import fresto.data.DataUnit;
import fresto.data.Pedigree;
import fresto.data.ApplicationDataUnit;
import fresto.data.EntryInvokeID;
import fresto.data.EntryInvokePropertyValue;
import fresto.data.EntryInvokeProperty;
import fresto.data.EntryReturnID;
import fresto.data.EntryReturnPropertyValue;
import fresto.data.EntryReturnProperty;
import fresto.data.HostID;
import fresto.data.ApplicationID;
import fresto.data.ManagedResourceID;
import fresto.data.OperationID;
import fresto.data.ClientDataUnit;
import fresto.data.ClientID;
import fresto.data.ClientPropertyValue;
import fresto.data.ClientProperty;
import fresto.data.ReferrerID;
import fresto.data.RequestID;
import fresto.data.RequestPropertyValue;
import fresto.data.RequestProperty;
import fresto.data.ResponseID;
import fresto.data.ResponsePropertyValue;
import fresto.data.ResponseProperty;
import fresto.data.ResourceID;
import fresto.data.ResourcePropertyValue;
import fresto.data.ResourceProperty;
import fresto.data.ClientRequestEdge;
import fresto.data.RequestResourceEdge;
import fresto.data.ResourceResponseEdge;
import fresto.data.ResponseClientEdge;

import fresto.format.UIEvent;
import fresto.command.CommandEvent;

public class UIEventWriter {
	private static String THIS_CLASS_NAME = "UIEventWriter";
	private static final String HDFS_URL = "hdfs://fresto1.owlab.com:9000/fresto/new";
	private static Logger LOGGER = Logger.getLogger(THIS_CLASS_NAME);
	private static final String ZMQ_URL = "tcp://fresto1.owlab.com:7004";
	private static final String TOPIC_REQUEST = "U";
	private static final String TOPIC_COMMAND_EVENT = "CMD";
	private static TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
	private static final SplitFrestoDataPailStructure pailStructure = new SplitFrestoDataPailStructure();
	private TypedRecordOutputStream tros;

	public static void main(String[] args) throws Exception {
		UIEventWriter eventWriter = new UIEventWriter();

		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket subscriber = context.socket(ZMQ.PULL);
		subscriber.connect(ZMQ_URL);
		//subscriber.subscribe("A".getBytes());
		//subscriber.subscribe(TOPIC_REQUEST.getBytes());
		//subscriber.subscribe(TOPIC_COMMAND_EVENT.getBytes());

		while(true) {
		//for(int i = 0; i < 10; i++){
			LOGGER.info("Waiting...");
			String topic = new String(subscriber.recv(0));
			//LOGGER.info("Topic=" + topic + " and matching=" + TOPIC_REQUEST.equals(topic));

			byte[] eventBytes = subscriber.recv(0);
			LOGGER.info(eventBytes.length + " bytes received");

			if(TOPIC_COMMAND_EVENT.equals(topic)) {
				LOGGER.info("A command received.");
				CommandEvent event = new CommandEvent();
				deserializer.deserialize(event, eventBytes);
				if(event.target_module.equalsIgnoreCase(THIS_CLASS_NAME)) {
					if(event.command.equalsIgnoreCase("exit")) {
						LOGGER.info("Perform command: " + event.command);
						break;
					} else {
						LOGGER.warning("Unsupported command: " + event.command);
						LOGGER.warning("Supported: exit");
					}
				}
			}

			// Append Pail Data
			long startTime = System.currentTimeMillis();
			eventWriter.openPail();
			eventWriter.appendPailData(topic, eventBytes, System.currentTimeMillis());
			eventWriter.closePail();
			LOGGER.info("Time taken for writing: " + (System.currentTimeMillis() - startTime) + " ms");
		}

		subscriber.close();
		context.term();
	}

	public void createPail() {
		try {
			if(tros == null) {
				Pail<FrestoData> pail = Pail.create(HDFS_URL, pailStructure);
				tros = pail.openWrite();
			}
		} catch(Exception e){
			throw new RuntimeException(e);
		}
	}

	public void openPail() {
		try {
			if(tros == null) {
				Pail<FrestoData> pail = new Pail<FrestoData>(HDFS_URL);
				tros = pail.openWrite();
			}
		} catch(IOException e){
			//throw new RuntimeException(e);
			//e.printStackTrace();
			LOGGER.info("Pail open failed, trying to create pail.");
			createPail();
		} catch(IllegalArgumentException e){
			//throw new RuntimeException(e);
			//e.printStackTrace();
			LOGGER.info("Pail open failed, trying to create pail.");
			createPail();
		}
	}

	public void closePail() {
		try {
			if(tros != null) 
				tros.close();
			tros = null;
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void appendPailData(String topic, byte[] eventBytes, long receive_timestamp) throws TException, IOException {
		if(TOPIC_REQUEST.equals(topic)) {

			UIEvent event = new UIEvent();
			deserializer.deserialize(event, eventBytes);

			LOGGER.fine("Event.stage : " + event.stage);
			LOGGER.fine("Event.clientId : " + event.clientId);
			LOGGER.fine("Event.currentPlace : " + event.currentPlace);
			LOGGER.fine("Event.uuid : " + event.uuid);
			LOGGER.fine("Event.url : " + event.url);
			LOGGER.fine("Event.timestamp : " + event.timestamp);
			LOGGER.fine("Event.httpStatus : " + event.httpStatus);
			// Ignore elapsedTime deliverately

			if("beforeCall".equals(event.stage)) {
				// Making ClientProperty
				ClientID clientId = ClientID.client_ip(event.clientId);

				ClientPropertyValue clientPropertyValue = new ClientPropertyValue();
				clientPropertyValue.setUser_agent("Dummy Agent");

				ClientProperty clientProperty = new ClientProperty();
				clientProperty.client = clientId;
				clientProperty.property = clientPropertyValue;

				// Making Requst Property
				RequestID requestId = RequestID.uuid(event.uuid);

				RequestPropertyValue requestPropertyValue = new RequestPropertyValue();
				requestPropertyValue.referrer = ReferrerID.url(event.currentPlace);
				requestPropertyValue.method = "GET";
				requestPropertyValue.query = event.url.split("\\?")[1];
				requestPropertyValue.timestamp = event.timestamp;
				
				RequestProperty requestProperty = new RequestProperty();
				requestProperty.request = requestId;
				requestProperty.property = requestPropertyValue;
				
				// Making Resource Property
				ResourceID resourceId = ResourceID.url(event.url);

				ResourcePropertyValue resourcePropertyValue = new ResourcePropertyValue();

				ResourceProperty resourceProperty = new ResourceProperty();
				resourceProperty.resource = resourceId;
				resourceProperty.property = resourcePropertyValue;

				// Making Client Request Edge
				ClientRequestEdge clientRequestEdge = new ClientRequestEdge();
				clientRequestEdge.client = clientId;
				clientRequestEdge.request = requestId;

				// Making Request Resource Edge
				RequestResourceEdge requestResourceEdge = new RequestResourceEdge();
				requestResourceEdge.request = requestId;
				requestResourceEdge.resource = resourceId;

				// Making one Pedigree
				Pedigree pedigree = new Pedigree();
				pedigree.fresto_timestamp = receive_timestamp;

				// Making Client Data Unit
				ClientDataUnit clientDataUnit = new ClientDataUnit();
				clientDataUnit.client_property = clientProperty;
				clientDataUnit.request_property = requestProperty;
				clientDataUnit.resource_property = resourceProperty;
				clientDataUnit.client_request_edge = clientRequestEdge;;
				clientDataUnit.request_resource_edge = requestResourceEdge;;
				// Making required Fresto Data objects 
				FrestoData fd = new FrestoData();
				fd.pedigree = pedigree;
				fd.data_unit = DataUnit.client_data_unit(clientDataUnit);

				tros.writeObject(fd);
				//fd.data_unit = DataUnit.client_data_unit(ClientDataUnit.client_property(clientProperty));
				//tros.writeObject(fd);

				//fd.data_unit = DataUnit.client_data_unit(ClientDataUnit.request_property(requestProperty));
				//tros.writeObject(fd);

				//fd.data_unit = DataUnit.client_data_unit(ClientDataUnit.resource_property(resourceProperty));
				//tros.writeObject(fd);

				//fd.data_unit = DataUnit.client_data_unit(ClientDataUnit.client_request_edge(clientRequestEdge));
				//tros.writeObject(fd);

				//fd.data_unit = DataUnit.client_data_unit(ClientDataUnit.request_resource_edge(requestResourceEdge));
				//tros.writeObject(fd);

			} else if("afterCall".equals(event.stage)) {
				// Making Client Property
				ClientID clientId = ClientID.client_ip(event.clientId);

				ClientPropertyValue clientPropertyValue = new ClientPropertyValue();
				clientPropertyValue.setUser_agent("Dummy Agent");

				ClientProperty clientProperty = new ClientProperty();
				clientProperty.client = clientId;
				clientProperty.property = clientPropertyValue;

				// Making Response Property
				ResponseID responseId = ResponseID.uuid(event.uuid);

				ResponsePropertyValue responsePropertyValue = new ResponsePropertyValue();
				responsePropertyValue.statusCode = 200;
				responsePropertyValue.length = 0L;
				responsePropertyValue.timestamp = event.timestamp;
				
				ResponseProperty responseProperty = new ResponseProperty();
				responseProperty.response = responseId;
				responseProperty.property = responsePropertyValue;
				
				// Making Resource Property
				ResourceID resourceId = ResourceID.url(event.url);

				ResourcePropertyValue resourcePropertyValue = new ResourcePropertyValue();

				ResourceProperty resourceProperty = new ResourceProperty();
				resourceProperty.resource = resourceId;
				resourceProperty.property = resourcePropertyValue;

				// Making Resource Response Edge
				ResourceResponseEdge resourceResponseEdge = new ResourceResponseEdge();
				resourceResponseEdge.resource = resourceId;
				resourceResponseEdge.response = responseId;

				// Making Response Clinet Edge
				ResponseClientEdge responseClientEdge = new ResponseClientEdge();
				responseClientEdge.response = responseId;
				responseClientEdge.client = clientId;

				// Making one Pedigree
				Pedigree pedigree = new Pedigree();
				pedigree.fresto_timestamp = receive_timestamp;

				// Making Client Data Unit
				ClientDataUnit clientDataUnit = new ClientDataUnit();
				clientDataUnit.client_property = clientProperty;
				clientDataUnit.response_property = responseProperty;
				clientDataUnit.resource_property = resourceProperty;
				clientDataUnit.resource_response_edge = resourceResponseEdge;;
				clientDataUnit.response_client_edge = responseClientEdge;;
				// Making required Fresto Data objects 
				FrestoData fd = new FrestoData();
				fd.pedigree = pedigree;
				fd.data_unit = DataUnit.client_data_unit(clientDataUnit);

				tros.writeObject(fd);
				//fd.data_unit = DataUnit.client_data_unit(ClientDataUnit.client_property(clientProperty));
				//tros.writeObject(fd);

				//fd.data_unit = DataUnit.client_data_unit(ClientDataUnit.response_property(responseProperty));
				//tros.writeObject(fd);

				//fd.data_unit = DataUnit.client_data_unit(ClientDataUnit.resource_property(resourceProperty));
				//tros.writeObject(fd);

				//fd.data_unit = DataUnit.client_data_unit(ClientDataUnit.resource_response_edge(resourceResponseEdge));
				//tros.writeObject(fd);

				//fd.data_unit = DataUnit.client_data_unit(ClientDataUnit.response_client_edge(responseClientEdge));
				//tros.writeObject(fd);
			} 
		} else {
				LOGGER.warning("Event topic: " + topic + " not recognized"); 
		}
	}
}
