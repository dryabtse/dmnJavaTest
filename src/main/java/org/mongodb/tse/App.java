package org.mongodb.tse;

import com.mongodb. * ;
import com.mongodb.client. * ;
import com.mongodb.client.model. * ;
import org.bson.Document;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class App {

	public static void printHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("org.mongodb.tse.App", options);
		System.exit(0);
	}

	public static MongoCollection getCollection(CommandLine cline, MongoDatabase database, String collectionParameter) {
		MongoCollection collection = null;
		if (cline.hasOption("readPreference")) {
			String p = cline.getOptionValue("readPreference");
			collection = database.getCollection(collectionParameter).withReadPreference(ReadPreference.valueOf(p));
		}
		else collection = database.getCollection(collectionParameter).withReadPreference(ReadPreference.secondaryPreferred());
		return collection;
	}

	public static void main(final String[] args) throws InterruptedException {

		Option help = Option.builder("help")
				.argName("help")
				.desc("get help")
				.build();
		Option ouri = Option.builder("uri")
				.argName("uri")
				.desc("mongodb uri, required")
				.hasArg().
				type(String.class)
				.build();
		Option odatabase = Option.builder("database")
				.argName("database")
				.desc("mongodb database, default productpersistdb")
				.hasArg()
				.type(String.class).build();
		Option ocollection = Option.builder("collection")
				.argName("collection")
				.desc("mongodb collection, default product")
				.hasArg()
				.type(String.class)
				.build();
		Option osleep = Option.builder("sleep")
				.argName("sleep")
				.desc("sleep between runs, default 10 seconds")
				.hasArg()
				.type(Integer.class)
				.build();
		Option othreads = Option.builder("threads")
				.argName("threads")
				.desc("number of threads to run, default 5")
				.hasArg()
				.type(Integer.class)
				.build();
		Option readPreference = Option.builder("readPreference")
				.argName("readPreference")
				.desc("read preference, default is secondaryPreferred")
				.hasArg()
				.type(String.class)
				.build();

		Options options = new Options();
		options.addOption(help);
		options.addOption(ouri);
		options.addOption(odatabase);
		options.addOption(ocollection);
		options.addOption(osleep);
		options.addOption(othreads);
		options.addOption(readPreference);

		CommandLineParser parser = new DefaultParser();
		CommandLine cline = null;
		try {
			// parse the command line arguments
			cline = parser.parse(options, args);
		}
		catch(ParseException exp) {
			// oops, something went wrong
			System.err.println("Parsing failed.  Reason: " + exp.getMessage());
		}

		if (args.length == 0 || cline.hasOption("help") ) {
			printHelp(options);
		}
		String defaultUri = "mongodb://dmn-shardingtest-0.dryabtsev-test.4125.mongodbdns.com:17017,dmn-apitest-0.dryabtsev-test.4125.mongodbdns.com:17017";
		String defaultCol = "col";
		String uri = cline.getOptionValue("uri", defaultUri);
		String database = cline.getOptionValue("database", "java");
		String collection = cline.getOptionValue("collection", defaultCol);
		System.out.println("Using database: " + database + " and collection: " + collection);

		MongoClient client = new MongoClient(new MongoClientURI(uri));
		MongoDatabase db = client.getDatabase(database);
		final MongoCollection < Document > c = db.getCollection(collection);

		int threads = 500;
		final long sleep = 100;

		for (int i = 0; i < threads; i++) {
			c.bulkWrite(
				Arrays.asList(new InsertOneModel < Document > (new Document("x", i).append("y", 0)), 
						new InsertOneModel < Document > (new Document("x", i).append("y", 1)), 
						new InsertOneModel < Document > (new Document("x", i).append("y", 2)), 
						new InsertOneModel < Document > (new Document("x", i).append("y", 3)), 
						new InsertOneModel < Document > (new Document("x", i).append("y", 4)), 
						new InsertOneModel < Document > (new Document("x", i).append("y", 5)), 
						new InsertOneModel < Document > (new Document("x", i).append("y", 6)), 
						new InsertOneModel < Document > (new Document("x", i).append("y", 7)), 
						new InsertOneModel < Document > (new Document("x", i).append("y", 8)), 
						new InsertOneModel < Document > (new Document("x", i).append("y", 9)), 
						new UpdateOneModel < Document > (new Document("x", i).append("y", 0), 
						new Document("$set", new Document("z", "updated")))), 
				new BulkWriteOptions().ordered(false));
		} // for
		ExecutorService pool = Executors.newFixedThreadPool(threads);

		for (int k = 0; k < threads; k++) {
			final int i = k;
			pool.execute(new Runnable() {
				public void run() {
					int count = 0;
					for (;;) {
						c.bulkWrite(
						Arrays.asList(
						new UpdateOneModel < Document > (new Document("x", i).append("y", 0), new Document("$set", new Document("z", "updated"))), 
							new UpdateOneModel < Document > (new Document("x", i).append("y", 1), new Document("$set", new Document("z", "updated"))), 
							new UpdateOneModel < Document > (new Document("x", i).append("y", 2), new Document("$set", new Document("z", "updated"))), 
							new UpdateOneModel < Document > (new Document("x", i).append("y", 3), new Document("$set", new Document("z", "updated"))), 
							new UpdateOneModel < Document > (new Document("x", i).append("y", 4), new Document("$set", new Document("z", "updated"))), 
							new UpdateOneModel < Document > (new Document("x", i).append("y", 5), new Document("$set", new Document("z", "updated"))), 
							new UpdateOneModel < Document > (new Document("x", i).append("y", 6), new Document("$set", new Document("z", "updated"))), 
							new UpdateOneModel < Document > (new Document("x", i).append("y", 7), new Document("$set", new Document("z", "updated"))), 
							new UpdateOneModel < Document > (new Document("x", i).append("y", 8), new Document("$set", new Document("z", "updated"))), 
							new UpdateOneModel < Document > (new Document("x", i).append("y", 9), new Document("$set", new Document("z", "updated")))));

						try {
							Thread.sleep(sleep);
						} catch(InterruptedException e) {}

						count++;

						if (count == 100) {
							System.out.println("Thread " + i + ": 1000 updates executed");
							count = 0;
						}
					} // for
				} // run()
			} // Runnable()
			);
		} // for
	}
}
