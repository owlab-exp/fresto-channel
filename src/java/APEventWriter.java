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


public class APEventWriter {

	private static Logger LOGGER = Logger.getLogger("APEventWriter");
	private static final String TOPIC_ENTRY_INVOKE = "HB";
	private static final String TOPIC_ENTRY_RETURN = "HE";
	private static TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
	private static final String path = "hdfs://fresto1.owlab.com:9000/fresto/new";
	private static final SplitFrestoDataPailStructure pailStructure = new SplitFrestoDataPailStructure();
	private TypedRecordOutputStream tros;

	public static void main(String[] args) throws Exception {
		APEventWriter eventWriter = new APEventWriter();
		eventWriter.openPail();

		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
		subscriber.connect("tcp://fresto1.owlab.com:7003");
		//subscriber.subscribe("A".getBytes());
		subscriber.subscribe(TOPIC_ENTRY_INVOKE.getBytes());
		subscriber.subscribe(TOPIC_ENTRY_RETURN.getBytes());

		while(true) {
			LOGGER.info("Waiting...");
			String topic = new String(subscriber.recv(0));
			byte[] eventBytes = subscriber.recv(0);
			LOGGER.info(eventBytes.length + " bytes received");

			// Append Pail Data
			eventWriter.appendPailData(topic, eventBytes, System.currentTimeMillis());
		}

		//eventWriter.closePail();
	}

	public void openPail() {
		try {
			if(tros == null) {
				Pail<FrestoData> pail = Pail.create(path, pailStructure);
				tros = pail.openWrite();
			}
		} catch(Exception e){
			throw new RuntimeException(e);
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

	public void appendPailData(String topic, byte[] eventBytes, long timestamp) throws TException, IOException {
		if(TOPIC_ENTRY_INVOKE.equals(topic)) {
			HttpRequestEvent event = new HttpRequestEvent();
			deserializer.deserialize(event, eventBytes);

			LOGGER.fine("Event.httpMethod : " + event.httpMethod);
			LOGGER.fine("Event.localHost : " + event.localHost);
			LOGGER.fine("Event.localPort : " + event.localPort);
			LOGGER.fine("Event.contextPath : " + event.contextPath);
			LOGGER.fine("Event.servletPath : " + event.servletPath);
			LOGGER.fine("Event.frestoUUID : " + event.frestoUUID);
			LOGGER.fine("Event.typeName : " + event.typeName);
			LOGGER.fine("Event.signatureName : " + event.signatureName);
			LOGGER.fine("Event.depth : " + event.depth);
			LOGGER.fine("Event.timestamp : " + event.timestamp);

			EntryInvokePropertyValue eipv = new EntryInvokePropertyValue();
			eipv.http_method = event.httpMethod;
			eipv.host = HostID.host_name(event.localHost);
			eipv.port = event.localPort;
			eipv.application = ApplicationID.context_path(event.contextPath);
			eipv.managed_resource = ManagedResourceID.servlet_path(event.servletPath);
			eipv.operation = OperationID.operation_name(event.signatureName);
			eipv.timestamp = event.timestamp;

			EntryInvokeProperty eip = new EntryInvokeProperty();
			eip.entry_invoke = EntryInvokeID.uuid(event.frestoUUID);
			eip.property = eipv;

			Pedigree pedigree = new Pedigree();
			pedigree.fresto_timestamp = timestamp;

			FrestoData fd = new FrestoData();
			fd.pedigree = pedigree;
			fd.data_unit = DataUnit.application_data_unit(ApplicationDataUnit.entry_invoke_property(eip));

			tros.writeObject(fd);
		}
		if(TOPIC_ENTRY_RETURN.equals(topic)) {
			HttpResponseEvent event = new HttpResponseEvent();
			deserializer.deserialize(event, eventBytes);
			
			LOGGER.info("Message Topic: " + topic);
			LOGGER.info("Event.frestoUUID : " + event.frestoUUID);
			LOGGER.info("Event.typeName : " + event.typeName);
			LOGGER.info("Event.signatureName : " + event.signatureName);
			LOGGER.info("Event.depth : " + event.depth);
			LOGGER.info("Event.timestamp : " + event.timestamp);
			LOGGER.info("Event.receivedTime : " + event.receivedTime);

			EntryReturnPropertyValue erpv = new EntryReturnPropertyValue();
			erpv.response_code = 200;
			erpv.length = -1L;
			erpv.timestamp = event.timestamp;


			EntryReturnProperty erp = new EntryReturnProperty();
			erp.entry_return = EntryReturnID.uuid(event.frestoUUID);
			erp.property = erpv;

			Pedigree pedigree = new Pedigree();
			pedigree.fresto_timestamp = timestamp;

			FrestoData fd = new FrestoData();
			fd.pedigree = pedigree;
			fd.data_unit = DataUnit.application_data_unit(ApplicationDataUnit.entry_return_property(erp));

			tros.writeObject(fd);
		}
			
			
	}
}

