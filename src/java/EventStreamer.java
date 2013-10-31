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

public  class EventStreamer extends  Thread implements Runnable {
	private static Logger LOGGER = Logger.getLogger("EventStreamer");

	private static int frontPort = 7001;
	private static int backPort = 7002;

	public  static void main(String[] args) {
		final ZMQ.Context context = ZMQ.context(1);

		final Thread zmqThread = new Thread() {
			@Override
			public void run() {

				//ZMQ.Socket frontEnd = context.socket(ZMQ.XSUB);
				ZMQ.Socket frontEnd = context.socket(ZMQ.SUB);
				frontEnd.connect("tcp://*:" + frontPort);
				frontEnd.subscribe("".getBytes());

				//ZMQ.Socket backEnd = context.socket(ZMQ.XPUB);
				ZMQ.Socket backEnd = context.socket(ZMQ.PUSH);
				backEnd.bind("tcp://*:" + backPort);

				LOGGER.info("Starting Streamer with " + frontPort + "/" + backPort);

				
				// Working!
				//ZMQ.proxy(frontEnd, backEnd, null);
				
				// Working!
				ZMQ.device(ZMQ.STREAMER, frontEnd, backEnd);

				frontEnd.close();
				backEnd.close();
			}
		};

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				LOGGER.info("An interrupt received. Shutting down server...");
				context.term();

				try {
					zmqThread.interrupt();
					zmqThread.join();
				} catch(InterruptedException e) {
				}
			}
		});

		zmqThread.start();
	}
}
