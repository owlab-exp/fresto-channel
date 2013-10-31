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
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import java.util.logging.Logger;

public  class EventHub extends  Thread implements Runnable {
	private static String THIS_CLASS_NAME = "EventHub";
	private static Logger LOGGER = Logger.getLogger(THIS_CLASS_NAME);

	private static int frontPort = 7000; // 7000
	private static int backPort = 7001; // 7001
	private ZMQ.Context ctx;

	public  static void main(String[] args) {
		final ZMQ.Context context = ZMQ.context(1);

		final Thread zmqThread = new Thread() {
			@Override
			public void run() {

				//ZMQ.Socket frontEnd = context.socket(ZMQ.XSUB);
				ZMQ.Socket frontEnd = context.socket(ZMQ.SUB);
				frontEnd.bind("tcp://*:" + frontPort);

				//ZMQ.Socket backEnd = context.socket(ZMQ.XPUB);
				ZMQ.Socket backEnd = context.socket(ZMQ.PUB);
				backEnd.bind("tcp://*:" + backPort);

				LOGGER.info("Starting Forwarder with " + frontPort + "/" + backPort);

				frontEnd.subscribe("".getBytes());
				
				// Working!
				//ZMQ.proxy(frontEnd, backEnd, null);
				
				// Working!
				ZMQ.device(ZMQ.FORWARDER, frontEnd, backEnd);

				frontEnd.close();
				backEnd.close();
			}
		};

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				LOGGER.info("Interrupt received. Shuting down server");
				context.term();
				
				try {
					zmqThread.interrupt();
					zmqThread.join();
				} catch(InterruptedException e) {
					LOGGER.warning("Exception occurred: " + e.getMessage());
				}
			}
		});

		zmqThread.start();
		//context.term();
	}
}
