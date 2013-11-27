package enar.voldemort;

import java.util.HashMap;
import java.util.Map;

import voldemort.client.StoreClient;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

/*
 * This class demonstrates a very simple usage of the 
 * Project Voldemort key value store.
 * 
 * For this code, it is sufficient to run with a single
 * database node, but note that Project Voldemort can 
 * be easily configured to use several nodes together in 
 * a cluster. As such, we're using the files under
 * config/single_node_cluster/config to specify our 
 * database node configuration.
 * 
 * Run this code using the Voldemort.launch configuration
 * in Eclipse, as it requires an environment variables 
 * to be set. (Right-click the .launch file and then select 
 * Run as > Voldemort). The environment variable tells the 
 * code to use the database node configuration specified in
 * config/single_node_cluster 
 * 
 * A very good overview of Project Voldemort can be found at:
 * http://www.project-voldemort.com/voldemort/design.html
 */
@SuppressWarnings({ "rawtypes", "unchecked", "unused" })
public class Voldemort {

	public static void main(String[] args) {
		new Voldemort().run();
	}

	private Connection connection = new Connection();

	public void run() {
		try {
			// Connect to the "test" store, which is configured
			// in config/single_node_cluster/config/stores.xml
			StoreClient store = connection.getStore("test");
			
			// Start by reading this code, running it, changing and rerunning it
			simpleReadAndWrite(store);
			
			// Once you're comfortable with the first method, comment in this
			// method, read the source, run it, change and rerun it, etc.
			inconsistentWrite(store);

		} finally {
			// Ensure that we dispose of the connection to the database
			// even if an exception is raised during the execution of
			// our program
			connection.finalise();
		}
	}

	private void simpleReadAndWrite(StoreClient store) {
		// Create an initial version of a "book" object
		// Note that we're storing data as a Map (called a
		// dictionary or associate array in other programming
		// languages).
		Map<String, Object> philosophersStone = createBook(
				"Potter and the Philosopher's Stone",
				"Rowling"
		);

		// Store our book in the Project Voldemort database
		// under the key "potter1"
		store.put("potter1", philosophersStone);

		// Retrieve our book from the database and print it out
		System.out.println("The value for key potter1 is:");
		System.out.println(store.get("potter1"));
	}
	
	private void inconsistentWrite(StoreClient store) {
		// The following code allows us to simulate a potential
		// consistency issue. We'll obtain two copies of our
		// book from the database, change them both at the same
		// time and then write them back to the database.
		//
		// In the real world, we'd expect these writes to be 
		// happening on two different clients.
		//
		// Read the code and refer to the Voldemort documentation.
		// What does the Versioned class do? Why is it needed?
		//
		// What happens when we run the code? How do we resolve the 
		// problem? What does the Project Voldemort design document say 
		// about this?
		//
		// What would have happened if the second write had been
		// received by a replica that hadn't yet been notified 
		// of the first write?
					
		Versioned<Map<String, Object>> firstPhilosophersStone = store.get("potter1");
		Versioned<Map<String, Object>> secondPhilosophersStone = store.get("potter1");
		
		firstPhilosophersStone.getValue().put("title", "Harry Potter and the Philosopher's Stone");
		secondPhilosophersStone.getValue().put("author", "J.K. Rowling");
		
		store.put("potter1", firstPhilosophersStone);
		store.put("potter1", secondPhilosophersStone);
	}

	private Map<String, Object> createBook(String title, String author) {
		Map<String, Object> book = new HashMap<String, Object>();
		book.put("title", title);
		book.put("author", author);
		return book;
	}
}
