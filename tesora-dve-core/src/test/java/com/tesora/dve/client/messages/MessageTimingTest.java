// OS_STATUS: public
package com.tesora.dve.client.messages;

import com.tesora.dve.comms.client.messages.GenericResponse;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.agent.Envelope;
import com.tesora.dve.smqplugin.SimpleMQPlugin;
import com.tesora.dve.worker.agent.Agent;

/**
 * A little test class to test the performance of the messaging system's raw
 * performance.
 * 
 */

public enum MessageTimingTest {
	
	INSTANCE;
	
	static class Caller extends Agent {

		private static final int MSGCOUNT = 1000;

		public Caller() throws PEException {
			super("caller");
		}

		@Override
		public void onMessage(Envelope e) throws PEException {
		}
		
		public void execute() throws PEException {
			Callee callee = new Callee();
			long sendtime = 0;
			long receivetime = 0;
			long totaltime = 0;
			Envelope e = newEnvelope("Test");
			e.from(getReplyAddress()).to(callee.getAddress());
			for (int i = 0; i < MSGCOUNT; ++i) {
				long start = System.nanoTime();
				send(e);
				sendtime += (System.nanoTime() - start);
				long receivestart = System.nanoTime();
				receive();
				long donetime = System.nanoTime();
				receivetime += donetime - receivestart;
				totaltime += donetime - start;
			}
			System.out.println("Avg microsecond per message:");
			System.out.println("sendtime " + sendtime / 1000 / MSGCOUNT);
			System.out.println("receivetime " + receivetime / 1000 / MSGCOUNT);
			System.out.println("totaltime " + totaltime / 1000 / MSGCOUNT);
			System.out.println("callee "+ callee.runtime / 1000 / MSGCOUNT);
			System.out.println("");
			System.out.println("callee / total = " + ((double)callee.runtime / totaltime));
			callee.close();
		}
	}
	
	static class Callee extends Agent {
		
		public long getRuntime() {
			return runtime;
		}

		long runtime = 0;

		public Callee() throws PEException {
			super("callee");
		}

		@Override
		public void onMessage(Envelope e) throws PEException {
			try {
				long t = System.nanoTime();
				Thread.sleep(10);
				runtime += (System.nanoTime() - t);
				returnResponse(e, new GenericResponse());
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
		
	}
	
	public static void main(String[] args) throws PEException {
		Agent.setPluginProvider(SimpleMQPlugin.class);
		Caller caller = new MessageTimingTest.Caller();
		caller.execute();
		caller.close();
	}

}
