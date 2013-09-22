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
import fresto.data.EntryReturnEdge;
import fresto.data.HostID;
import fresto.data.ApplicationID;
import fresto.data.ManagedResourceID;
import fresto.data.OperationID;
import fresto.data.TypeID;
import fresto.data.OperationProperty;
import fresto.data.OperationPropertyValue;
import fresto.data.EntryInvokeEdge;
import fresto.data.ApplicationResourceEdge;
import fresto.data.HostApplicationEdge;
import fresto.data.ImplementResourceEdge;

import fresto.command.CommandEvent;

public class APEventWriter {
	private static String THIS_CLASS_NAME = "APEventWriter";
	private static Logger LOGGER = Logger.getLogger(THIS_CLASS_NAME);
	private static final String ZMQ_URL = "tcp://fresto1.owlab.com:7005";
	private static final String TOPIC_ENTRY_INVOKE = "HB";
	private static final String TOPIC_ENTRY_RETURN = "HE";
	private static final String TOPIC_COMMAND_EVENT = "CMD";
	private static final String HDFS_PATH = "hdfs://fresto1.owlab.com:9000/fresto/new";
	private static final SplitFrestoDataPailStructure pailStructure = new SplitFrestoDataPailStructure();
	private TypedRecordOutputStream tros;
	private static TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());

	public static void main(String[] args) throws Exception {
		APEventWriter eventWriter = new APEventWriter();

		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket puller = context.socket(ZMQ.PULL);
		puller.connect(ZMQ_URL);

		//puller.subscribe(TOPIC_ENTRY_INVOKE.getBytes());
		//puller.subscribe(TOPIC_ENTRY_RETURN.getBytes());
		//puller.subscribe(TOPIC_COMMAND_EVENT.getBytes());

		while(true) {
		//for(int i = 0; i < 10; i++){
			LOGGER.info("Waiting...");
			String topic = new String(puller.recv(0));
			
			byte[] eventBytes = puller.recv(0);
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
			LOGGER.info("Time taken: " + (System.currentTimeMillis() - startTime) + " ms");	
		}

		puller.close();
		context.term();
	}

	public void createPail() {
		try {
			if(tros == null) {
				Pail<FrestoData> pail = Pail.create(HDFS_PATH, pailStructure);
				tros = pail.openWrite();
			}
		} catch(Exception e){
			throw new RuntimeException(e);
		}
	}

	public void openPail() {
		try {
			if(tros == null) {
				Pail<FrestoData> pail = new Pail<FrestoData>(HDFS_PATH);
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

	public void appendPailData(String topic, byte[] eventBytes, long frestoTimestamp) throws TException, IOException {
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

			HostID hostId = HostID.host_name(event.localHost);
			ApplicationID applicationId = ApplicationID.context_path(event.contextPath);
			ManagedResourceID managedResourceId = ManagedResourceID.servlet_path(event.servletPath);
			TypeID typeId = TypeID.type_name(event.typeName);

			OperationID operationId = OperationID.operation_name(event.signatureName);
			OperationPropertyValue operationPropertyValue = new OperationPropertyValue();
			operationPropertyValue.type = typeId;

			OperationProperty operationProperty = new OperationProperty();
			operationProperty.operation = operationId;
			operationProperty.property = operationPropertyValue;

			EntryInvokeID entryInvokeId = EntryInvokeID.uuid(event.frestoUUID);


			EntryInvokePropertyValue entryInvokePropertyValue = new EntryInvokePropertyValue();
			entryInvokePropertyValue.http_method = event.httpMethod;
			entryInvokePropertyValue.host = hostId;
			entryInvokePropertyValue.port = event.localPort;
			entryInvokePropertyValue.application = applicationId;
			entryInvokePropertyValue.managed_resource = managedResourceId;
			entryInvokePropertyValue.operation = operationId;
			entryInvokePropertyValue.timestamp = event.timestamp;

			EntryInvokeProperty entryInvokeProperty = new EntryInvokeProperty();
			entryInvokeProperty.entry_invoke = entryInvokeId;
			entryInvokeProperty.property = entryInvokePropertyValue;

			EntryInvokeEdge entryInvokeEdge = new EntryInvokeEdge();
			entryInvokeEdge.entry_invoke = entryInvokeId;
			entryInvokeEdge.operation = operationId;

			ImplementResourceEdge implementResourceEdge = new ImplementResourceEdge();
			implementResourceEdge.managed_resource = managedResourceId;
			implementResourceEdge.operation = operationId;

			ApplicationResourceEdge applicationResourceEdge = new ApplicationResourceEdge();
			applicationResourceEdge.application = applicationId;
			applicationResourceEdge.managed_resource = managedResourceId;

			HostApplicationEdge hostApplicationEdge = new HostApplicationEdge();
			hostApplicationEdge.host = hostId;
			hostApplicationEdge.application = applicationId;

			ApplicationDataUnit applicationDataUnit = new ApplicationDataUnit();
			applicationDataUnit.entry_invoke_property = entryInvokeProperty;
			applicationDataUnit.entry_invoke_edge = entryInvokeEdge;
			applicationDataUnit.operation_property = operationProperty;
			applicationDataUnit.implement_resource_edge = implementResourceEdge;
			applicationDataUnit.application_resource_edge = applicationResourceEdge;
			applicationDataUnit.host_application_edge = hostApplicationEdge;
			
			Pedigree pedigree = new Pedigree();
			pedigree.fresto_timestamp = frestoTimestamp;

			FrestoData fd = new FrestoData();
			fd.pedigree = pedigree;
			fd.data_unit = DataUnit.application_data_unit(applicationDataUnit);

			tros.writeObject(fd);

			//fd.data_unit = DataUnit.application_data_unit(ApplicationDataUnit.entry_invoke_property(entryInvokeProperty));
			//tros.writeObject(fd);

			//fd.data_unit = DataUnit.application_data_unit(ApplicationDataUnit.entry_invoke_edge(entryInvokeEdge));
			//tros.writeObject(fd);

			//fd.data_unit = DataUnit.application_data_unit(ApplicationDataUnit.operation_property(operationProperty));
			//tros.writeObject(fd);

			//fd.data_unit = DataUnit.application_data_unit(ApplicationDataUnit.implement_resource_edge(implementResourceEdge));
			//tros.writeObject(fd);

			//fd.data_unit = DataUnit.application_data_unit(ApplicationDataUnit.application_resource_edge(applicationResourceEdge));
			//tros.writeObject(fd);

			//fd.data_unit = DataUnit.application_data_unit(ApplicationDataUnit.host_application_edge(hostApplicationEdge));
			//tros.writeObject(fd);



		} else if(TOPIC_ENTRY_RETURN.equals(topic)) {
			HttpResponseEvent event = new HttpResponseEvent();
			deserializer.deserialize(event, eventBytes);
			
			LOGGER.fine("Event.frestoUUID : " + event.frestoUUID);
			LOGGER.fine("Event.typeName : " + event.typeName);
			LOGGER.fine("Event.signatureName : " + event.signatureName);
			LOGGER.fine("Event.depth : " + event.depth);
			LOGGER.fine("Event.timestamp : " + event.timestamp);
			LOGGER.fine("Event.receivedTime : " + event.receivedTime);

			EntryReturnID entryReturnId = EntryReturnID.uuid(event.frestoUUID);
			EntryReturnPropertyValue entryReturnPropertyValue = new EntryReturnPropertyValue();
			entryReturnPropertyValue.response_code = 200;
			entryReturnPropertyValue.length = -1L;
			entryReturnPropertyValue.timestamp = event.timestamp;


			EntryReturnProperty entryReturnProperty = new EntryReturnProperty();
			entryReturnProperty.entry_return = entryReturnId;
			entryReturnProperty.property = entryReturnPropertyValue;

			OperationID operationId = OperationID.operation_name(event.signatureName);
			TypeID typeId = TypeID.type_name(event.typeName);
			OperationPropertyValue operationPropertyValue = new OperationPropertyValue();
			operationPropertyValue.type = typeId;
			
			OperationProperty operationProperty = new OperationProperty();
			operationProperty.operation = operationId;
			operationProperty.property = operationPropertyValue;

			EntryReturnEdge entryReturnEdge = new EntryReturnEdge();
			entryReturnEdge.entry_return = entryReturnId;
			entryReturnEdge.operation = operationId;

			ApplicationDataUnit applicationDataUnit = new ApplicationDataUnit();
			applicationDataUnit.entry_return_property = entryReturnProperty;
			applicationDataUnit.operation_property = operationProperty;
			applicationDataUnit.entry_return_edge = entryReturnEdge;
			
			Pedigree pedigree = new Pedigree();
			pedigree.fresto_timestamp = frestoTimestamp;

			FrestoData fd = new FrestoData();
			fd.pedigree = pedigree;
			fd.data_unit = DataUnit.application_data_unit(applicationDataUnit);

			tros.writeObject(fd);

			//fd.data_unit = DataUnit.application_data_unit(ApplicationDataUnit.entry_return_property(entryReturnProperty));
			//tros.writeObject(fd);

			//fd.data_unit = DataUnit.application_data_unit(ApplicationDataUnit.operation_property(operationProperty));
			//tros.writeObject(fd);

			//fd.data_unit = DataUnit.application_data_unit(ApplicationDataUnit.entry_return_edge(entryReturnEdge));
			//tros.writeObject(fd);

		} else {
			LOGGER.info("Topic not processed: " + topic);
		}
	}
}

