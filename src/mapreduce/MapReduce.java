package mapreduce;

import java.lang.ProcessBuilder.Redirect;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/*
 * A class which is the starting point for the actual execution of 
 * MapReduce jobs. As of now, the workers are initialized
 * in the execute() function of this class, and their individual jobs
 * are executed manually in sequence here.
 */
public class MapReduce {
	private Class mapperClass;
	private Class reducerClass;
	private MapReduceSpecification spec;
	private ServerSocket masterSocket;
	private static final int PORT = 9002;

	public MapReduce(MapReduceSpecification spec, Class mapperClass, Class reducerClass) {
		this.mapperClass = mapperClass;
		this.reducerClass = reducerClass;
		this.spec = spec;
	}

	public int execute() throws IOException {
		// This is only an approximation: see
		// https://stackoverflow.com/questions/12807797/java-get-available-memory
		long freeMemory = Runtime.getRuntime().freeMemory();

		// Set up the byte offsets for the map workers (commented out since apparently
		// workers can do this)
		long fileSize = Files.size(spec.getInput());

		// Create dirs for intermediate files
		Path outputDir = spec.getOutput();
		Path intermediateDir = outputDir.resolve("intermediate");
		File dir = new File(intermediateDir.toString());
		if (!dir.mkdir()) {
			System.out.println("Error: Could not make intermediate file directory.");
			throw new IOException();
		}

		// Set up our N mappers do their thing!
		File mapperLog = new File("mapper_log.txt");
		ProcessBuilder[] pb = new ProcessBuilder[spec.getN()];
		Process[] mapperProcs = new Process[spec.getN()];
		ExecutorService pool = Executors.newFixedThreadPool(spec.getN());

		masterSocket = new ServerSocket(PORT);
		// masterSocket.setSoTimeout(1000); // Timeout is in milliseconds

		HashMap<Integer, ClientHandler> clients = new HashMap<Integer, ClientHandler>(spec.getN());
		Queue<ClientHandler> unfinished = new LinkedList<ClientHandler>();

		for (int i = 0; i < spec.getN(); i++) {
			System.out.printf("Setting up mapper %d\n", i);

			pb[i] = new ProcessBuilder("java", "-classpath", "target/classes", this.mapperClass.getName(),
					this.mapperClass.getName(), Integer.toString(spec.getN()), Integer.toString(i),
					Long.toString(fileSize), intermediateDir.toString(), spec.getInput().toString());
			pb[i].redirectErrorStream(true);
			pb[i].redirectOutput(Redirect.appendTo(mapperLog));
			mapperProcs[i] = pb[i].start();
			// System.out.printf("Process id is: %d\n", p.pid());

			Socket client = masterSocket.accept();

			ClientHandler curr = new ClientHandler(client, i);

			clients.put(i, curr);
			pool.execute(curr);
			unfinished.add(curr);
		}

		/*
		 * Idea is to have a new runnable ClientHandler class for each mapper running on
		 * different threads, each terminating once they receive a message from the
		 * mapper delivering the locations of the intermediate files.
		 */
		while (!unfinished.isEmpty()) {
			// In here we restart failed mappers until all finished
			ClientHandler curr = unfinished.poll();
			int k = curr.getWorkerNum();
			if (curr.isFinished() && !curr.successful()) {
				// Need to restart
				System.out.printf("Restarting mapper %d\n", k);
				mapperProcs[k].destroy();

				pb[k] = new ProcessBuilder("java", "-classpath", "target/classes", this.mapperClass.getName(),
						this.mapperClass.getName(), Integer.toString(spec.getN()), Integer.toString(k),
						Long.toString(fileSize), intermediateDir.toString(), spec.getInput().toString());
				pb[k].redirectErrorStream(true);
				pb[k].redirectOutput(Redirect.appendTo(mapperLog));
				mapperProcs[k] = pb[k].start();

				Socket client = masterSocket.accept(); // TODO Currently assuming accept() never times out

				ClientHandler newHandler = new ClientHandler(client, k);

				clients.replace(k, newHandler);
				pool.execute(newHandler);

				unfinished.add(newHandler);
			} else if (!curr.isFinished()) { // Not finished, put back on the queue
				// System.out.printf("Worker %d still unfinished\n", k);
				unfinished.add(curr);
			}

		}

		String[][] fps = new String[spec.getN()][spec.getN()];
		for (int i = 0; i < spec.getN(); i++) {
			for (int j = 0; j < spec.getN(); j++) { // Iterate over workers and get their i-th filepath
				fps[i][j] = clients.get(j).getFilePaths()[i];
				//System.out.printf("Reducer worker %d is receiving file path %s\n", i, fps[i][j]);
			}
		}

		// Reducer
		Path finalDir = outputDir.resolve("final");
		File dir2 = new File(finalDir.toString());
		if (!dir2.mkdir()) {
			System.out.println("Error: Could not make final file directory.");
			throw new IOException();
		}

		Process[] reducerProcs = new Process[spec.getN()];
		File reducerLog = new File("reducer_log.txt");
		pool = Executors.newFixedThreadPool(spec.getN());
		for (int i = 0; i < spec.getN(); i++) {
			System.out.printf("Setting up reducer %d\n", i);

			/* Problem: Using a diff classpath here to call the main function in reducer. But
			 * this is probably making the external sorting library unreachable somehow.
			 */
			pb[i] = new ProcessBuilder("java", "-classpath", "target/classes:test/*", this.reducerClass.getName(),
					this.reducerClass.getName(), Integer.toString(i), intermediateDir.toString(), finalDir.toString());
			pb[i].redirectErrorStream(true);
			pb[i].redirectOutput(Redirect.appendTo(reducerLog));
			reducerProcs[i] = pb[i].start();

			Socket client = masterSocket.accept();
			
			// Send intermediate file paths to the worker
			PrintWriter out = new PrintWriter(client.getOutputStream(), true);
			String fp = "Filepaths\n" + String.join("\t", fps[i]);
			out.println(fp);

			ClientHandler curr = new ClientHandler(client, i);

			clients.replace(i, curr);
			pool.execute(curr);
			unfinished.add(curr);
		}
		
		while (!unfinished.isEmpty()) {
			// In here we restart failed mappers until all finished
			ClientHandler curr = unfinished.poll();
			int k = curr.getWorkerNum();
			if (curr.isFinished() && !curr.successful()) {
				// Need to restart
				//System.out.printf("Restarting worker %d\n", k);
				reducerProcs[k].destroy();

				pb[k] = new ProcessBuilder("java", "-classpath", "target/classes:test/*", this.reducerClass.getName(),
						this.reducerClass.getName(), Integer.toString(k), intermediateDir.toString(), finalDir.toString());
				pb[k].redirectErrorStream(true);
				pb[k].redirectOutput(Redirect.appendTo(reducerLog));
				reducerProcs[k] = pb[k].start();

				Socket client = masterSocket.accept(); // TODO Currently assuming accept() never times out
				
				// Send intermediate file paths to the worker
				PrintWriter out = new PrintWriter(client.getOutputStream(), true);
				String fp = "Filepaths\n" + String.join("\t", fps[k]);
				out.println(fp);

				ClientHandler newHandler = new ClientHandler(client, k);

				clients.replace(k, newHandler);
				pool.execute(newHandler);
				unfinished.add(newHandler);
			} else if (!curr.isFinished()) { // Not finished, put back on the queue
				//System.out.printf("Worker %d still unfinished\n", k);
				unfinished.add(curr);
			}
		}
		
		masterSocket.close();
		
		pool.shutdown();
		try {
			pool.awaitTermination(2, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return 0;
	}

}