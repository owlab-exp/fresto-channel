/**************************************************************************************
 * Copyright 2013 TheSystemIdeas, Inc and Contributors. All rights reserved.          *
 *                                                                                    *
 *     https://github.com/owlab/fresto                                                *
 *                                                                                    *
 *                                                                                    *
 * ---------------------------------------------------------------------------------- *
 * This file is licensed under the Apache License, Version 2.0 (the "License");       *
 * you may not use this file except in compliance with the License.                   *
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 * 
 **************************************************************************************/
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
import fresto.data.SqlID;
import fresto.data.RequestEdge;
import fresto.data.ResponseEdge;
import fresto.data.EntryOperationCallEdge;
import fresto.data.EntryOperationReturnEdge;
import fresto.data.OperationCallEdge;
import fresto.data.OperationReturnEdge;
import fresto.data.SqlCallEdge;
import fresto.data.SqlReturnEdge;
import fresto.command.CommandEvent;

//import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
//import com.tinkerpop.blueprints.impls.orient.OrientGraph;
//import com.tinkerpop.blueprints.Graph;
//import com.tinkerpop.blueprints.Vertex;
//import com.tinkerpop.blueprints.Edge;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
//import com.orientechnologies.orient.core.db.graph.OGraphDatabasePool;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;

import org.perf4j.javalog.JavaLogStopWatch;
import org.perf4j.StopWatch;

public class OrientEventWriter {
	private static String THIS_CLASS_NAME = "OrientEventWriter";
	private static Logger LOGGER = Logger.getLogger(THIS_CLASS_NAME);

	//private static final String ZMQ_URL = "tcp://fresto1.owlab.com:7002";

	//private static final String DB_URL = "remote:fresto3.owlab.com/frestodb";
	//private static final String DB_USER = "admin";
	//private static final String DB_PASSWORD = "admin";

	private static String frontHost;
	private static int frontPort;
	private static String dbHost;
	private static String dbName;
	private static String dbUser;
	private static String password;

	// Client Events
	private static final String TOPIC_REQUEST = "CB";
	private static final String TOPIC_RESPONSE = "CF";

	// Server Events
	private static final String TOPIC_ENTRY_CALL = "EB";
	private static final String TOPIC_ENTRY_RETURN = "EF";
	private static final String TOPIC_OPERATION_CALL = "OB";
	private static final String TOPIC_OPERATION_RETURN = "OF";
	private static final String TOPIC_SQL_CALL = "SB";
	private static final String TOPIC_SQL_RETURN = "SF";

	// Command Events
	private static final String TOPIC_COMMAND_EVENT = "CMD";
	
	//private FrestoData frestoData = new FrestoData();
	private static TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
	

	private static boolean work = true;
	private static boolean sleepOn = false;
	private static int SLEEP_TIME = 10;

	private static OGraphDatabase oGraph;

	public OrientEventWriter() {
		//this.oGraph = setupDBConnection();

	}

	//public OrientEventWriter(String frontHost, int frontPort, String dbHost, String dbName, String dbUser, String password) {
	//	this.frontHost = frontHost;
	//	this.frontPort = frontPort;
	//	this.dbHost = dbHost;
	//	this.dbName = dbName;
	//	this.dbUser = dbUser;
	//	this.password = password;
	//}

	public static void main(String[] args) throws Exception {
		if(args.length <  6) {
			LOGGER.severe("Argumests needed : <frontHost> <frontPort> <dbHost> <dbName> <dbUser> <password>");
			System.exit(1);
		} else {
			try { 
				frontHost = args[0];
				frontPort = Integer.parseInt(args[1]); 
				dbHost = args[2];
				dbName = args[3];
				dbUser = args[4];
				password = args[5];
			} catch(NumberFormatException e) {
				LOGGER.severe("frontPort shouldb be an integer.");
				System.exit(1);
			}
		}

		final ZMQ.Context context = ZMQ.context(1);

		final FrestoEventQueue frestoEventQueue = new FrestoEventQueue();

		final Thread writerThread = new Thread() {
			@Override
			public void run() {
				Logger _LOGGER = Logger.getLogger("writerThread");
				StopWatch _watch = new JavaLogStopWatch(_LOGGER);

				OrientEventWriter eventWriter = new OrientEventWriter();


				// Open database
				//_LOGGER.info("Setup DB Connection");
				eventWriter.setupDBConnection();

				ZMQ.Socket puller = context.socket(ZMQ.PULL);
				puller.connect("tcp://" + frontHost + ":" + frontPort);

				//Consume socket data
				//FrestoEventQueue frestoEventQueue = new FrestoEventQueue(puller);
				frestoEventQueue.setPullerSocket(puller);
				frestoEventQueue.start();

				while(work) {

					// To add sufficient events to the queue
					if(sleepOn) {
						try {
							Thread.sleep(SLEEP_TIME);
						} catch(InterruptedException ie) {
						}
					}

					int queueSize = frestoEventQueue.size();
					
					if(queueSize > 0) {

						//eventWriter.setupDBConnection();
						_watch.start();
						oGraph.open(dbUser, password);
						_watch.lap("Write", queueSize + " events to be processed");

						try { // for database close finally

							for(int i = 0; i < queueSize; i++) {
								FrestoEvent frestoEvent = frestoEventQueue.poll(); 
								//To shutting down gracefully by using ZMQ but not used.
								//if(TOPIC_COMMAND_EVENT.equals(frestoEvent.topic)) {
								//	eventWriter.handleCommand(frestoEvent.topic, frestoEvent.eventBytes);
								//	continue;
								//}
								try {
									eventWriter.writeEventData(frestoEvent.topic, frestoEvent.eventBytes);
								} catch(Exception te) {
									_LOGGER.warning("Exception occurred: " + te.getMessage());
								}
							}
						} finally {
							oGraph.close();
						}

						// Count this
						//oGraph.declareIntent(null);

						_watch.stop("Write", queueSize + " events processed");
						//LOGGER.info(queueSize + " events processed");
					} else {
						_LOGGER.fine("No events.");

					}

				}
				_LOGGER.info("Shutting down...");


				//oGraph.close();

				puller.close();
				context.term();

				_LOGGER.info("Good bye.");
			}
		};

		Runtime.getRuntime().addShutdownHook(new Thread() {
         		@Override
         		public void run() {
         		   System.out.println("Interrupt received, killing server¡¦");
			   // To break while clause
			   frestoEventQueue.stopWork();
			   work = false;

         		  try {
				  frestoEventQueue.join();
				  writerThread.join();

         		  } catch (InterruptedException e) {
         		  }
         		}
      		});

		writerThread.start();

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

	public OGraphDatabase setupDBConnection() {
		//OGraphDatabase oGraph = new OGraphDatabase(DB_URL);
		LOGGER.info("Setting up connection to DB"); 
		oGraph = new OGraphDatabase("remote:"+ dbHost + "/" + dbName);
		oGraph.setProperty("minPool", 1);
		oGraph.setProperty("maxPool", 3);

		LOGGER.info("oGraph=" + oGraph); 
		return oGraph;
	}

	public void writeEventData(String topic, byte[] eventBytes) throws TException, IOException {
		if(TOPIC_REQUEST.equals(topic) 
			|| TOPIC_RESPONSE.equals(topic)
			|| TOPIC_ENTRY_CALL.equals(topic)
			|| TOPIC_ENTRY_RETURN.equals(topic)
			|| TOPIC_OPERATION_CALL.equals(topic)
			|| TOPIC_OPERATION_RETURN.equals(topic)
			|| TOPIC_SQL_CALL.equals(topic)
			|| TOPIC_SQL_RETURN.equals(topic)
			) {

			FrestoData frestoData = new FrestoData();
			////_watch.lap("frestoData new");
			//Reuse
			//frestoData.clear();
			deserializer.deserialize(frestoData, eventBytes);
			////_watch.lap("deserialize eventBytes");

			Pedigree pedigree = new Pedigree();
                        pedigree.setReceivedTime(System.currentTimeMillis());

                        frestoData.setPedigree(pedigree);
			////_watch.lap("setting pedigree");

			if(frestoData.dataUnit.isSetRequestEdge()) {

				RequestEdge requestEdge = frestoData.dataUnit.getRequestEdge();
				ClientID clientId = requestEdge.clientId;
				ResourceID resourceId = requestEdge.resourceId;

				//StopWatch _watch = new LoggingStopWatch("Writing Request Event");

				ODocument request = oGraph.createVertex("Request")
					.field("clientIp", clientId.getClientIp())
					.field("url", resourceId.getUrl())
					.field("referrer", requestEdge.referrer)
					.field("method", requestEdge.method)
					.field("timestamp", requestEdge.timestamp)
					.field("uuid", requestEdge.uuid)
					.save();

				//_watch.lap("Request event processed");
				linkToTS(oGraph, request.getIdentity(), "request", requestEdge.timestamp);
				//_watch.stop("Link event processed");

			} else if(frestoData.dataUnit.isSetResponseEdge()) {

				ResponseEdge responseEdge = frestoData.dataUnit.getResponseEdge();
				ClientID clientId = responseEdge.clientId;
				ResourceID resourceId = responseEdge.resourceId;

				//StopWatch _watch = new LoggingStopWatch("Writing Response Event");

				ODocument response = oGraph.createVertex("Response")
					.field("clientIp", clientId.getClientIp())
					.field("url", resourceId.getUrl())
					.field("httpStatus", responseEdge.httpStatus)
					.field("elapsedTime", responseEdge.elapsedTime)
					.field("timestamp", responseEdge.timestamp)
					.field("uuid", responseEdge.uuid)
					.save();

				//_watch.lap("Response event processed");
				linkToTS(oGraph, response.getIdentity(), "response", responseEdge.timestamp);
				//_watch.stop("Link event processed");

			} else if(frestoData.dataUnit.isSetEntryOperationCallEdge()) {

				EntryOperationCallEdge entryOperationCallEdge = frestoData.dataUnit.getEntryOperationCallEdge();
				ResourceID resourceId = entryOperationCallEdge.resourceId;
				OperationID operationId = entryOperationCallEdge.operationId;

				//StopWatch _watch = new LoggingStopWatch("Writing EntryOperationCall");

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
					.field("depth", entryOperationCallEdge.depth)
					.save();


				//_watch.lap("EntryOperationCall event processed");
				linkToTS(oGraph, entryCall.getIdentity(), "entryCall", entryOperationCallEdge.timestamp);
				//_watch.stop("Link event processed");




			} else if(frestoData.dataUnit.isSetEntryOperationReturnEdge()) {

				EntryOperationReturnEdge entryOperationReturnEdge = frestoData.dataUnit.getEntryOperationReturnEdge();
				ResourceID resourceId = entryOperationReturnEdge.resourceId;
				OperationID operationId = entryOperationReturnEdge.operationId;

				//StopWatch _watch = new LoggingStopWatch("Writing EntryOperationReturn");

				ODocument entryReturn = oGraph.createVertex("EntryOperationReturn")
					.field("servletlPath", entryOperationReturnEdge.servletPath)
					.field("operationName", operationId.getOperationName())
					.field("typeName", operationId.getTypeName())
					.field("httpStatus", entryOperationReturnEdge.httpStatus)
					.field("timestamp", entryOperationReturnEdge.timestamp)
					.field("elapsedTime", entryOperationReturnEdge.elapsedTime)
					.field("uuid", entryOperationReturnEdge.uuid)
					.field("sequence", entryOperationReturnEdge.sequence)
					.field("depth", entryOperationReturnEdge.depth)
					.save();

				//_watch.lap("EntryOperationReturn event processed");
				linkToTS(oGraph, entryReturn.getIdentity(), "entryReturn", entryOperationReturnEdge.timestamp);
				//_watch.stop("Link event processed");


			} else if(frestoData.dataUnit.isSetOperationCallEdge()) {

				OperationCallEdge operationCallEdge = frestoData.dataUnit.getOperationCallEdge();
				OperationID operationId = operationCallEdge.operationId;

				//StopWatch _watch = new LoggingStopWatch("Writing OperationCall");

				ODocument operationCall = oGraph.createVertex("OperationCall")
					.field("operationName", operationId.getOperationName())
					.field("typeName", operationId.getTypeName())
					.field("timestamp", operationCallEdge.timestamp)
					.field("uuid", operationCallEdge.uuid)
					.field("depth", operationCallEdge.depth)
					.field("sequence", operationCallEdge.sequence)
					.save();

				//_watch.lap("OperationCall event processed");
				linkToTS(oGraph, operationCall.getIdentity(), "operationCall", operationCallEdge.timestamp);
				//_watch.stop("Link event processed");

			} else if(frestoData.dataUnit.isSetOperationReturnEdge()) {
				OperationReturnEdge operationReturnEdge = frestoData.dataUnit.getOperationReturnEdge();
				OperationID operationId = operationReturnEdge.operationId;

				//StopWatch _watch = new LoggingStopWatch("Writing OperationReturn");

				ODocument operationReturn = oGraph.createVertex("OperationReturn")
					.field("operationName", operationId.getOperationName())
					.field("typeName", operationId.getTypeName())
					.field("timestamp", operationReturnEdge.timestamp)
					.field("elapsedTime", operationReturnEdge.elapsedTime)
					.field("uuid", operationReturnEdge.uuid)
					.field("sequence", operationReturnEdge.sequence)
					.field("depth", operationReturnEdge.depth)
					.save();

				//_watch.lap("OperationReturn event processed");
				linkToTS(oGraph, operationReturn.getIdentity(), "operationReturn", operationReturnEdge.timestamp);
				//_watch.stop("Link event processed");
			} else if(frestoData.dataUnit.isSetSqlCallEdge()) {

				SqlCallEdge sqlCallEdge = frestoData.dataUnit.getSqlCallEdge();
				SqlID sqlId = sqlCallEdge.sqlId;

				//StopWatch _watch = new LoggingStopWatch("Writing SqlCall");

				ODocument sqlCall = oGraph.createVertex("SqlCall")
					.field("databaseUrl", sqlId.getDatabaseUrl())
					.field("sql", sqlId.getSql())
					.field("timestamp", sqlCallEdge.timestamp)
					.field("uuid", sqlCallEdge.uuid)
					.field("depth", sqlCallEdge.depth)
					.field("sequence", sqlCallEdge.sequence)
					.save();

				//_watch.lap("SqlCall event processed");
				linkToTS(oGraph, sqlCall.getIdentity(), "sqlCall", sqlCallEdge.timestamp);
				//_watch.stop("Link event processed");

			} else if(frestoData.dataUnit.isSetSqlReturnEdge()) {
				SqlReturnEdge sqlReturnEdge = frestoData.dataUnit.getSqlReturnEdge();
				SqlID sqlId = sqlReturnEdge.sqlId;

				//StopWatch _watch = new LoggingStopWatch("Writing SqlReturn");

				ODocument sqlReturn = oGraph.createVertex("SqlReturn")
					.field("databaseUrl", sqlId.getDatabaseUrl())
					.field("sql", sqlId.getSql())
					.field("timestamp", sqlReturnEdge.timestamp)
					.field("elapsedTime", sqlReturnEdge.elapsedTime)
					.field("uuid", sqlReturnEdge.uuid)
					.field("depth", sqlReturnEdge.depth)
					.field("sequence", sqlReturnEdge.sequence)
					.save();

				//_watch.lap("SqlReturn event processed");
				linkToTS(oGraph, sqlReturn.getIdentity(), "sqlReturn", sqlReturnEdge.timestamp);
				//_watch.stop("Link event processed");
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
			// TODO how not to get map  object? I just want to know if the second key exists
			Map<String, ODocument> secondMap = rootDoc.field("second");
			//LOGGER.info("secondMap size = " + secondMap.size());
			ODocument secondDoc = secondMap.get(second);
			if(secondDoc != null) {
				// a map reated to the second exists
				OCommandSQL cmd = new OCommandSQL();
				cmd.setText("UPDATE " + secondDoc.getIdentity() + " ADD " + property + " = " + oRID);
				int updated = oGraph.command(cmd).execute();
			} else {
				// a map reated to the second does not exist
				OCommandSQL cmd = new OCommandSQL();
				//cmd.setText("INSERT INTO TSSecond (request, response, entryCall, entryReturn) values ([],[],[],[])");
				cmd.setText("INSERT INTO TSSecond (request, response, entryCall, entryReturn, operationCall, operationReturn, sqlCall, sqlReturn) values ([], [], [], [], [],[],[],[])");
				secondDoc = oGraph.command(cmd).execute();

				cmd.setText("UPDATE " + rootDoc.getIdentity() + " PUT second = \"" + second + "\", " + secondDoc.getIdentity());
				int updated = oGraph.command(cmd).execute();
				
				linkToTS(oGraph, oRID, property, timestamp);

			}

		} else {
			LOGGER.info("Creating TSSecond vertex...");
			OCommandSQL cmd = new OCommandSQL();
			//cmd.setText("insert into TSSecond (request, response, entryCall, entryReturn) values ([],[],[],[])");
			cmd.setText("INSERT INTO TSSecond (request, response, entryCall, entryReturn, operationCall, operationReturn, sqlCall, sqlReturn) values ([], [], [], [], [],[],[],[])");
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

