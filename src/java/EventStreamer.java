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

	private static String FRONT_HOST = "*";
	private static int FRONT_PORT = 7001;
	private static int BACK_PORT = 7002;

	public  static void main(String[] args) {

 		if(args.length < 3) {
            LOGGER.info("Possible arguments: <front host> <front port> <back port>");
            LOGGER.info("Default front host and front/back ports will be used.");
        } else {
			FRONT_HOST = args[0];
            try {
                FRONT_PORT = Integer.parseInt(args[1]);
                BACK_PORT = Integer.parseInt(args[2]);
            } catch(NumberFormatException nfe) {
                System.err.println("Port arguments must be integers.");
                System.exit(1);
            }
        }
		final ZMQ.Context context = ZMQ.context(1);

		final Thread zmqThread = new Thread() {
			@Override
			public void run() {

				//ZMQ.Socket frontEnd = context.socket(ZMQ.XSUB);
				ZMQ.Socket frontEnd = context.socket(ZMQ.SUB);
				frontEnd.connect("tcp://" + FRONT_HOST + ":" + FRONT_PORT);
				frontEnd.subscribe("".getBytes());

				//ZMQ.Socket backEnd = context.socket(ZMQ.XPUB);
				ZMQ.Socket backEnd = context.socket(ZMQ.PUSH);
				backEnd.bind("tcp://*:" + BACK_PORT);

				LOGGER.info("Starting Streamer with " + FRONT_HOST + ":" + FRONT_PORT + "/" + BACK_PORT);

				
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
