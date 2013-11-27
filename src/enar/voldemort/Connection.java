package enar.voldemort;

import voldemort.client.MockStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.serialization.ObjectSerializer;
import voldemort.serialization.StringSerializer;


/**
 * The Connection class encapsulates the details needed to start
 * a Project Voldemort server and establish a connection to one 
 * of its stores.
 * 
 * You won't need to modify this class works, and you don't need
 * to understand how it works unless you're curious.
 */
public class Connection {
	
	private StoreClientFactory storeFactory;
	
	public Connection() {
		this.storeFactory = new MockStoreClientFactory(
			new StringSerializer(),
			new ObjectSerializer<Object>(),
			new StringSerializer()
		);
	}

	public StoreClient getStore(String storeName) {
		return storeFactory.getStoreClient(storeName);
	}

	public void finalise() {
		// noop
		// Implementation required if we ever switch back
		// to an implementation of StoreClientFactory that
		// hits a real server
	}
}
