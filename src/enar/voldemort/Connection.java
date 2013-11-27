package enar.voldemort;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;


/**
 * The Connection class encapsulates the details needed to start
 * a Project Voldemort server and establish a connection to one 
 * of its stores.
 * 
 * You won't need to modify this class works, and you don't need
 * to understand how it works unless you're curious.
 */
public class Connection {
	
	private VoldemortServer server;
	private StoreClientFactory storeFactory;
	
	public Connection() {
		this.server = initialiseServer();
		this.storeFactory = initialiseStoreFactory(this.server);
	}

	public StoreClient getStore(String storeName) {
		return storeFactory.getStoreClient(storeName);
	}

	public void finalise() {
		server.stop();
	}

	private VoldemortServer initialiseServer() {
		VoldemortConfig config = VoldemortConfig.loadFromEnvironmentVariable();
		VoldemortServer server = new VoldemortServer(config);

		if (!server.isStarted()) {
			server.start();
		}
		
		return server;
	}
	
	private StoreClientFactory initialiseStoreFactory(VoldemortServer server) {
		Node node = server.getIdentityNode();

		StoreClientFactory factory = new SocketStoreClientFactory(
				new ClientConfig().setBootstrapUrls("tcp://" + node.getHost()
						+ ":" + node.getSocketPort()));

		return factory;
	}
}
