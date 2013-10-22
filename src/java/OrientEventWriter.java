import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.logging.Logger;
import java.io.IOException;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import fresto.data.FrestoData;
import fresto.data.Pedigree;
import fresto.data.DataUnit;
import fresto.data.ClientID;
import fresto.data.ResourceID;
import fresto.data.OperationID;
import fresto.data.RequestEdge;
import fresto.data.ResponseEdge;
import fresto.data.EntryOperationCallEdge;
import fresto.data.EntryOperationReturnEdge;
import fresto.data.OperationCallEdge;
import fresto.data.OperationReturnEdge;
import fresto.command.CommandEvent;

//import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
//import com.tinkerpop.blueprints.impls.orient.OrientGraph;
//import com.tinkerpop.blueprints.Graph;
//import com.tinkerpop.blueprints.Vertex;
//import com.tinkerpop.blueprints.Edge;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;

import org.perf4j.LoggingStopWatch;
import org.perf4j.StopWatch;

public class OrientEventWriter {
	private static String THIS_CLASS_NAME = "OrientEventWriter";
	private static Logger LOGGER = Logger.getLogger(THIS_CLASS_NAME);

	private static final String ZMQ_URL = "tcp://fresto1.owlab.com:7002";

	private static final String DB_URL = "remote:fresto3.owlab.com/frestodb";
	private static final String DB_USER = "admin";
	private static final String DB_PASSWORD = "admin";

	// Client Events
	private static final String TOPIC_REQUEST = "CB";
	private static final String TOPIC_RESPONSE = "CF";

	// Server Events
	private static final String TOPIC_ENTRY_CALL = "EB";
	private static final String TOPIC_ENTRY_RETURN = "EF";
	private static final String TOPIC_OPERATION_CALL = "OB";
	private static final String TOPIC_OPERATION_RETURN = "OF";

	// Command Events
	private static final String TOPIC_COMMAND_EVENT = "CMD";
	
	//private FrestoData frestoData = new FrestoData();
	private static TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
	

	private static boolean work = true;
	private static boolean sleepOn = false;
	private static int SLEEP_TIME = 10;

	private static OGraphDatabase oGraph;

	public OrientEventWriter() {
		this.oGraph = openDatabase();

	}

	public static void main(String[] args) throws Exception {

		OrientEventWriter eventWriter = new OrientEventWriter();


		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket puller = context.socket(ZMQ.PULL);
		puller.connect(ZMQ_URL);

		//Consume socket data
		FrestoEventQueue frestoEventQueue = new FrestoEventQueue(puller);
		frestoEventQueue.start();

		while(work) {

			// To add sufficient events to the queue
			if(sleepOn)
				Thread.sleep(SLEEP_TIME);

			int queueSize = frestoEventQueue.size();
			
			if(queueSize > 0) {
				//oGraph.declareIntent(new OIntentMassiveInsert());

				for(int i = 0; i < queueSize; i++) {
					FrestoEvent frestoEvent = frestoEventQueue.poll(); 
					if(TOPIC_COMMAND_EVENT.equals(frestoEvent.topic)) {
						eventWriter.handleCommand(frestoEvent.topic, frestoEvent.eventBytes);
						continue;
					}
					eventWriter.writeEventData(frestoEvent.topic, frestoEvent.eventBytes);
				}

				// Count this
				//oGraph.declareIntent(null);

				LOGGER.info(queueSize + " events processed.");
			} else {
				LOGGER.info(queueSize + " events.");

			}

		}


		oGraph.close();

		puller.close();
		context.term();
	}

	public void handleCommand(String topic, byte[] eventBytes) throws TException {
		LOGGER.info("A command received."); 
		CommandEvent event = new CommandEvent(); 
		deserializer.deserialize(event, eventBytes); 
		if(event.target_module.equalsIgnoreCase(THIS_CLASS_NAME)) { 
			if(event.command.equalsIgnoreCase("exit")) { 
				LOGGER.info("Perform command: " + event.command); 
				work = false; 
			} else { 
				LOGGER.warning("Unsupported command: " + event.command); 
			} 
		}
	}

	public OGraphDatabase openDatabase() {
		OGraphDatabase oGraph = new OGraphDatabase(DB_URL);
		oGraph.open(DB_USER, DB_PASSWORD);
		//oGraph.setMaxBufferSize(0);
		//
		//oGraph.setLockMode(OGraphDatabase.LOCK_MODE.NO_LOCKING);
		//
		// Condider this afterward
		//oGraph.declareIntent(new OIntentMassiveInsert());
		//Not working
		//oGraph.setRetainObjects(false);

		return oGraph;
	}

	public void writeEventData(String topic, byte[] eventBytes) throws TException, IOException {
		if(TOPIC_REQUEST.equals(topic) 
			|| TOPIC_RESPONSE.equals(topic)
			|| TOPIC_ENTRY_CALL.equals(topic)
			|| TOPIC_ENTRY_RETURN.equals(topic)
			|| TOPIC_OPERATION_CALL.equals(topic)
			|| TOPIC_OPERATION_RETURN.equals(topic)
			) {

			//StopWatch _watch = new LoggingStopWatch("writeEventData");

			FrestoData frestoData = new FrestoData();
			//_watch.lap("frestoData new");
			//Reuse
			//frestoData.clear();
			deserializer.deserialize(frestoData, eventBytes);
			//_watch.lap("deserialize eventBytes");

			Pedigree pedigree = new Pedigree();
                        pedigree.setReceivedTime(System.currentTimeMillis());

                        frestoData.setPedigree(pedigree);
			//_watch.lap("setting pedigree");

			if(frestoData.dataUnit.isSetRequestEdge()) {

				RequestEdge requestEdge = frestoData.dataUnit.getRequestEdge();
				ClientID clientId = requestEdge.clientId;
				ResourceID resourceId = requestEdge.resourceId;

				StopWatch _watch = new LoggingStopWatch("Writing Request Event");

				ODocument request = oGraph.createVertex("Request")
					.field("clientIp", clientId.getClientIp())
					.field("url", resourceId.getUrl())
					.field("referrer", requestEdge.referrer)
					.field("method", requestEdge.method)
					.field("timestamp", requestEdge.timestamp)
					.field("uuid", requestEdge.uuid)
					.save();

				linkToTS(oGraph, request.getIdentity(), "request", requestEdge.timestamp);
				_watch.stop("Request event processed");

			} else if(frestoData.dataUnit.isSetResponseEdge()) {

				ResponseEdge responseEdge = frestoData.dataUnit.getResponseEdge();
				ClientID clientId = responseEdge.clientId;
				ResourceID resourceId = responseEdge.resourceId;

				StopWatch _watch = new LoggingStopWatch("Writing Response Event");

				ODocument response = oGraph.createVertex("Response")
					.field("clientIp", clientId.getClientIp())
					.field("url", resourceId.getUrl())
					.field("httpStatus", responseEdge.httpStatus)
					.field("elapsedTime", responseEdge.elapsedTime)
					.field("timestamp", responseEdge.timestamp)
					.field("uuid", responseEdge.uuid)
					.save();

				linkToTS(oGraph, response.getIdentity(), "response", responseEdge.timestamp);
				_watch.stop("Response event processed");

			} else if(frestoData.dataUnit.isSetEntryOperationCallEdge()) {

				EntryOperationCallEdge entryOperationCallEdge = frestoData.dataUnit.getEntryOperationCallEdge();
				ResourceID resourceId = entryOperationCallEdge.resourceId;
				OperationID operationId = entryOperationCallEdge.OperationId;

				StopWatch _watch = new LoggingStopWatch("Writing EntryOperationCall");

				ODocument entryCall = oGraph.createVertex("EntryOperationCall")
					.field("hostName", entryOperationCallEdge.localHost)
					.field("contextPath", entryOperationCallEdge.contextPath)
					.field("port", entryOperationCallEdge.localPort)
					.field("servletPath", entryOperationCallEdge.servletPath)
					.field("operationName", operationId.getOperationName())
					.field("typeName", operationId.getTypeName())
					.field("httpMethod", entryOperationCallEdge.httpMethod)
					.field("uuid", entryOperationCallEdge.uuid)
					.field("timestamp", entryOperationCallEdge.timestamp)
					.field("sequence", entryOperationCallEdge.sequence)
					.save();


				linkToTS(oGraph, entryCall.getIdentity(), "entryCall", entryOperationCallEdge.timestamp);

				_watch.stop("EntryOperationCall event processed");


			} else if(frestoData.dataUnit.isSetEntryOperationReturnEdge()) {

				EntryOperationReturnEdge entryOperationReturnEdge = frestoData.dataUnit.getEntryOperationReturnEdge();
				ResourceID resourceId = entryOperationReturnEdge.resourceId;
				OperationID operationId = entryOperationReturnEdge.operationId;

				StopWatch _watch = new LoggingStopWatch("Writing EntryOperationReturn");

				ODocument entryReturn = oGraph.createVertex("EntryOperationReturn")
					.field("servletlPath", entryOperationReturnEdge.servletPath)
					.field("operationName", operationId.getOperationName())
					.field("typeName", operationId.getTypeName())
					.field("httpStatus", entryOperationReturnEdge.httpStatus)
					.field("timestamp", entryOperationReturnEdge.timestamp)
					.field("elapsedTime", entryOperationReturnEdge.elapsedTime)
					.field("uuid", entryOperationReturnEdge.uuid)
					.save();

				linkToTS(oGraph, entryReturn.getIdentity(), "entryReturn", entryOperationReturnEdge.timestamp);

				_watch.stop("EntryOperationReturn event processed");

			} else if(frestoData.dataUnit.isSetOperationReturnEdge()) {
			} else {
				LOGGER.info("No data unit exist.");
			}

			
		} else {
			LOGGER.warning("Event topic: " + topic + " not recognized.");
		}
	}

        public static ODocument findOne(OGraphDatabase oGraph, OSQLSynchQuery oQuery, Map<String, Object> params) {
		List<ODocument> result = oGraph.command(oQuery).execute(params);
                for(ODocument doc: result) {
                                LOGGER.fine("Found.");
                                return doc;
                }
                return null;

        }

	public static ODocument lookForVertex(OGraphDatabase oGraph, String indexName, Object key) {
		ODocument vertex = null;
		OIndex<?> idx = oGraph.getMetadata().getIndexManager().getIndex(indexName);
		if(idx != null) {
			OIdentifiable rec = (OIdentifiable) idx.get(key);
			if(rec != null) {
				vertex = oGraph.getRecord(rec);
			} else {
				LOGGER.info("ORID: " + rec + " does not exist");
			}
		} else {
			LOGGER.info("INDEX: " + idx + " does not exist");
		}

		return vertex;
	}

	public static void linkToTS(OGraphDatabase oGraph, OIdentifiable oRID, String property, long timestamp) {
		long second = (timestamp/1000) * 1000;
		long minute = (timestamp/60000) * 60000;
		//String second = "" + ((timestamp/1000) * 1000);
		//String minute = "" + ((timestamp/60000) * 60000);

		OSQLSynchQuery<ODocument> oQuery = new OSQLSynchQuery<ODocument>();
		Map<String, Object> params = new HashMap<String, Object>();

		oQuery.setText("select from TSRoot where minute = :minute");
		params.put("minute", minute);
		List<ODocument> rootDocs = oGraph.command(oQuery).execute(params);

		if(rootDocs.size() > 0) {
			ODocument rootDoc = rootDocs.get(0);
			Map<String, ODocument> secondMap = rootDoc.field("second");
			LOGGER.info("secondMap size = " + secondMap.size());
			ODocument secondDoc = secondMap.get(second);
			if(secondDoc != null) {
				// a map reated to the second exists
				OCommandSQL cmd = new OCommandSQL();
				cmd.setText("UPDATE " + secondDoc.getIdentity() + " ADD " + property + " = " + oRID);
				int updated = oGraph.command(cmd).execute();
			} else {
				// a map reated to the second does not exist
				OCommandSQL cmd = new OCommandSQL();
				cmd.setText("INSERT INTO TSSecond (request, response, entryCall, entryReturn) values ([],[],[],[])");
				secondDoc = oGraph.command(cmd).execute();

				cmd.setText("UPDATE " + rootDoc.getIdentity() + " PUT second = \"" + second + "\", " + secondDoc.getIdentity());
				int updated = oGraph.command(cmd).execute();
				
				linkToTS(oGraph, oRID, property, timestamp);

			}

		} else {
			LOGGER.info("Creating TSSecond vertex...");
			OCommandSQL cmd = new OCommandSQL();
			cmd.setText("insert into TSSecond (request, response, entryCall, entryReturn) values ([],[],[],[])");
			ODocument newSecondDoc = oGraph.command(cmd).execute();

			LOGGER.info("Creating TSRoot vertex...");
			ODocument newTSDoc = oGraph.createVertex("TSRoot")
				.field("minute", minute)
				.save();

			cmd.setText("UPDATE " + newTSDoc.getIdentity() + " PUT second = \"" + second + "\", " + newSecondDoc.getIdentity());

			oGraph.command(cmd).execute();

			// call this method once again
			linkToTS(oGraph, oRID, property, timestamp);
		}
	}
}

