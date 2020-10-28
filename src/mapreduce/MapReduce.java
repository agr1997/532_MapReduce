package mapreduce;

import java.lang.ProcessBuilder.Redirect;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A class which is the starting point for the actual execution of MapReduce
 * jobs. Spawns the N mappers and reducers and manages communication and fault
 * tolerance.
 */
public class MapReduce {
	private Class mapperClass;
	private Class reducerClass;
	private MapReduceSpecification spec;
	private ServerSocket masterSocket;
	private static final int PORT = 9002;
	private static final int masterSocketTimeout = 500; // In milliseconds

	/**
	 * Constructor.
	 * 
	 * @param spec         - the specifications for this map reduce job
	 * @param mapperClass  - the actual class object for the UDF mapper
	 * @param reducerClass - the actual class object for the UDF reducer
	 */
	public MapReduce(MapReduceSpecification spec, Class mapperClass, Class reducerClass) {
		this.mapperClass = mapperClass;
		this.reducerClass = reducerClass;
		this.spec = spec;
	}

	/**
	 * Tries to shut down a thread pool in a graceful way, but resorts to force if
	 * too much time elapses.
	 * 
	 * @param pool - the thread pool to shut down
	 */
	public static void shutdownCarefully(ExecutorService pool) throws Exception {
		pool.shutdown(); // Try to shut down gracefully
		try {
			if (!pool.awaitTermination(3, TimeUnit.SECONDS)) {
				pool.shutdownNow(); // Force-quit, essentially
				if (!pool.awaitTermination(3, TimeUnit.SECONDS)) {
					System.err.println("Pool did not terminate.");
					// If pool could not terminate, fail loudly
					throw new Exception();
				}
			}
		} catch (InterruptedException ie) {
			pool.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}

	private void killProcs(Process[] procs) {
		int numToKill = procs.length;
		for (int i = 0; i < numToKill; i++) {
			if (procs[i].isAlive()) {
				procs[i].destroy();
				while (procs[i].isAlive()) {
					// Make sure it is dead
				}
			}

		}
	}

	/**
	 * Executes the MapReduce job.
	 * 
	 * @param args - possible arguments for fault tolerance testing
	 * @throws IOException
	 */
	public void execute(String[] args) throws IOException {
		long inputSize = Files.size(this.spec.getInput());
		int numWorkers = this.spec.getN();
		boolean testingFaultTolerance = false;

		// Parse arguments from the command line for fault tolerance testing and assert assumptions
		String workerNumToKill = null;
		String workerTypeToKill = null;
		int killWorkerN = -1;
		int sleepLength = 0;
		int killSection = -1; // could be 0,1
		if (args.length >= 5) {
			workerNumToKill = args[1];
			killWorkerN = Integer.parseInt(workerNumToKill);
			// Valid worker numbers are 0,1,...,(N-1)
			if (killWorkerN >= numWorkers || killWorkerN < 0) {
				System.err.printf("You want to kill worker %d, but there are N=%d workers for this job.\n", killWorkerN,
						spec.getN());
				throw new IOException();
			}

			workerTypeToKill = args[2];
			workerTypeToKill = workerTypeToKill.trim().toLowerCase();
			if (!workerTypeToKill.equals("mapper") && !workerTypeToKill.equals("reducer")) {
				System.err.printf("Worker type to kill must be either 'reducer' or 'mapper'. Got: %s\n",
						workerTypeToKill);
				throw new IOException();
			}

			sleepLength = Integer.parseInt(args[3]);
			if (!(sleepLength >= 0)) {
				System.err.printf("Sleep length must be a nonnegative integer.\n");
				throw new IOException();
			}

			killSection = Integer.parseInt(args[4]);
			if (!(killSection == 0) && !(killSection == 1)) {
				System.err.printf(
						"Valid choices for kill section are 0 (before socket connects) and 1 (after connection established).\n");
				throw new IOException();
			}

			// Always kill a worker process
			testingFaultTolerance = true;
		}

		// Create directories for intermediate files
		Path outputDir = this.spec.getOutput();
		Path intermediateDir = outputDir.resolve("intermediate");
		File dir = new File(intermediateDir.toString());
		if (!dir.mkdir()) {
			System.out.println("Error: Could not make intermediate file directory.");
			throw new IOException();
		}

		// Create a directory for log files for the workers.
		Path logDirPath = outputDir.resolve("log");
		File logDir = new File(logDirPath.toString());
		if (!logDir.mkdir()) {
			System.out.println("Error: Could not make log file directory.");
			throw new IOException();
		}

		/*
		 * Spawn new processes for the N mapper workers and execute ClientHandlers in
		 * new threads to perform communication between this process (master) and the
		 * worker processes.
		 */
		ProcessBuilder pb;
		Process[] mapperProcs = new Process[numWorkers];
		ExecutorService pool = Executors.newFixedThreadPool(numWorkers);

		masterSocket = new ServerSocket(PORT);
		masterSocket.setSoTimeout(masterSocketTimeout);

		HashMap<Integer, ClientHandler> handlers = new HashMap<Integer, ClientHandler>(numWorkers);
		Queue<ClientHandler> unfinished = new LinkedList<ClientHandler>();

		int c = 0; // A counter to use for fault tolerance testing
		boolean kill = false;
		int sleepFor = 0;
		for (int i = 0; i < numWorkers; i++) {
			Socket client = null;
			while (client == null) {
				try {
					System.out.printf("Setting up mapper %d.\n", i);

					kill = (testingFaultTolerance && workerTypeToKill.equals("mapper") && i == killWorkerN
							&& killSection >= 0);
					if (kill && c == 0) {
						sleepFor = sleepLength;
					} else {
						sleepFor = 0;
					}

					pb = new ProcessBuilder("java", "-classpath", "target/classes", this.mapperClass.getName(),
							this.mapperClass.getName(), Integer.toString(numWorkers), Integer.toString(i),
							Long.toString(inputSize), intermediateDir.toString(), this.spec.getInput().toString(),
							Integer.toString(sleepFor));
					pb.redirectErrorStream(true);
					String logFileName = logDirPath.resolve(String.format("mapper_%d_log.txt", i)).toString();
					pb.redirectOutput(Redirect.to(new File(logFileName)));
					mapperProcs[i] = pb.start();

					if (kill && c == 0 && killSection == 0) {
						mapperProcs[i].destroy();
						while (mapperProcs[i].isAlive()) {
							// Make sure the process is really dead
						}
						c++;
					}

					client = masterSocket.accept();
				} catch (SocketTimeoutException e) {
					System.out.printf(
							"Setup for mapper %d failed due to timeout while waiting for socket. Trying again.\n", i);
					mapperProcs[i].destroyForcibly();
					while (mapperProcs[i].isAlive()) {
						// Make sure the process is really dead
					}
				}
			}

			ClientHandler curr = new ClientHandler(client, i);
			handlers.put(i, curr);
			pool.execute(curr);
			unfinished.add(curr);
		}

		if (testingFaultTolerance && workerTypeToKill.equals("mapper") && killSection == 1) {
			mapperProcs[killWorkerN].destroy();
			while (mapperProcs[killWorkerN].isAlive()) {
				// Make sure the process is really dead
			}
		}

		/*
		 * Until the queue of ClientHandlers with unfinished business is empty, check
		 * their status and restart mappers if a handler is 'finished', but did not see
		 * successful completion of mapping task.
		 */
		while (!unfinished.isEmpty()) {
			ClientHandler curr = unfinished.poll();
			int k = curr.getWorkerNum();
			boolean finished = curr.isFinished();
			boolean successful = curr.successful();

			if (finished && !successful) {
				System.out.printf("Restarting mapper %d.\n", k);
				mapperProcs[k].destroy();
				while (mapperProcs[k].isAlive()) {
					// Make sure the process is really dead
				}

				Socket client = null;
				while (client == null) {
					try {
						pb = new ProcessBuilder("java", "-classpath", "target/classes", this.mapperClass.getName(),
								this.mapperClass.getName(), Integer.toString(numWorkers), Integer.toString(k),
								Long.toString(inputSize), intermediateDir.toString(), this.spec.getInput().toString(),
								"0");
						pb.redirectErrorStream(true);
						String logFileName = logDirPath.resolve(String.format("mapper_%d_log.txt", k)).toString();
						pb.redirectOutput(Redirect.to(new File(logFileName)));
						mapperProcs[k] = pb.start();

						client = masterSocket.accept();
					} catch (SocketTimeoutException e) {
						System.out.printf("Restart for mapper %d failed due to " + "timeout while waiting for socket. "
								+ "Trying again.\n", k);
						mapperProcs[k].destroyForcibly();
						while (mapperProcs[k].isAlive()) {
							// Make sure the process is really dead
						}
					}
				}

				ClientHandler newHandler = new ClientHandler(client, k);

				handlers.replace(k, newHandler);
				pool.execute(newHandler);
				unfinished.add(newHandler);
			} else if (!finished) {
				unfinished.add(curr);
			}
		}

		killProcs(mapperProcs); // Clean up procs
		// Ensure no old ClientHandlers are still running
		try {
			shutdownCarefully(pool);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		// Organize intermediate file paths received in the handlers
		String[][] fps = new String[numWorkers][numWorkers];
		for (int i = 0; i < numWorkers; i++) {
			for (int j = 0; j < numWorkers; j++) {
				fps[i][j] = handlers.get(j).getFilePaths()[i];
			}
		}

		// Create a directory for the final outputs from reducers
		Path finalDirPath = outputDir.resolve("final");
		File finalDir = new File(finalDirPath.toString());
		if (!finalDir.mkdir()) {
			System.out.println("Error: Could not make final file directory.");
			throw new IOException();
		}

		// Spawn the N reducer processes.
		c = 0;
		Process[] reducerProcs = new Process[numWorkers];
		pool = Executors.newFixedThreadPool(numWorkers);
		unfinished = new LinkedList<ClientHandler>();
		for (int i = 0; i < numWorkers; i++) {
			Socket client = null;
			BufferedReader in = null;
			PrintWriter out = null;
			while (client == null) {
				try {
					System.out.printf("Setting up reducer %d.\n", i);

					kill = (testingFaultTolerance && workerTypeToKill.equals("reducer") && i == killWorkerN
							&& killSection >= 0);
					if (kill && c == 0) {
						sleepFor = sleepLength;
					} else {
						sleepFor = 0;
					}

					pb = new ProcessBuilder("java", "-classpath", "target/classes:test/*", this.reducerClass.getName(),
							this.reducerClass.getName(), Integer.toString(i), intermediateDir.toString(),
							finalDir.toString(), Integer.toString(sleepFor));
					pb.redirectErrorStream(true);
					String logFileName = logDirPath.resolve(String.format("reducer_%d_log.txt", i)).toString();
					pb.redirectOutput(Redirect.to(new File(logFileName)));
					reducerProcs[i] = pb.start();

					if (kill && c == 0 && killSection == 0) {
						reducerProcs[i].destroy();
						while (reducerProcs[i].isAlive()) {
							// Make sure the process is really dead
						}
						c++;
					}

					client = masterSocket.accept();
				} catch (SocketTimeoutException e) {
					System.out.printf(
							"Setup for reducer %d failed due to timeout while waiting for socket. Trying again.\n", i);
					reducerProcs[i].destroyForcibly();
					while (reducerProcs[i].isAlive()) {
						// Make sure the process is really dead
					}
				}
			}

			// Send intermediate file paths to the worker
			out = new PrintWriter(client.getOutputStream(), true);
			String fp = "Filepaths\n" + String.join("\t", fps[i]);
			out.println(fp);

			in = new BufferedReader(new InputStreamReader(client.getInputStream()));
			ClientHandler curr = new ClientHandler(client, i, in, out, true);

			handlers.replace(i, curr);
			pool.execute(curr);
			unfinished.add(curr);
		}

		if (testingFaultTolerance && workerTypeToKill.equals("reducer") && killSection == 1) {
			reducerProcs[killWorkerN].destroy();
			while (reducerProcs[killWorkerN].isAlive()) {
				// Make sure the process is really dead
			}
		}

		// Restart reducer workers until all the work is complete
		while (!unfinished.isEmpty()) {
			ClientHandler curr = unfinished.poll();
			int k = curr.getWorkerNum();
			
			boolean finished = curr.isFinished();
			boolean successful = curr.successful();

			if (finished && !successful) {
				System.out.printf("Restarting reducer %d.\n", k);
				reducerProcs[k].destroy();
				while (reducerProcs[k].isAlive()) {
					// Make sure the process is really dead
				}

				Socket client = null;
				BufferedReader in = null;
				PrintWriter out = null;
				while (client == null) {
					try {
						pb = new ProcessBuilder("java", "-classpath", "target/classes:test/*",
								this.reducerClass.getName(), this.reducerClass.getName(), Integer.toString(k),
								intermediateDir.toString(), finalDir.toString(), "0");
						pb.redirectErrorStream(true);
						String logFileName = logDirPath.resolve(String.format("reducer_%d_log.txt", k)).toString();
						pb.redirectOutput(Redirect.to(new File(logFileName)));
						reducerProcs[k] = pb.start();

						client = masterSocket.accept();
					} catch (SocketTimeoutException e) {
						System.out.printf(
								"Restart for reducer %d failed due to timeout while waiting for socket. Trying again.\n",
								k);
						reducerProcs[k].destroyForcibly();
						while (reducerProcs[k].isAlive()) {
							// wait for it to die
						}
					}
				}

				// Send intermediate file paths to the worker
				out = new PrintWriter(client.getOutputStream(), true);
				String fp = "Filepaths\n" + String.join("\t", fps[k]);
				out.println(fp);

				in = new BufferedReader(new InputStreamReader(client.getInputStream()));
				ClientHandler newHandler = new ClientHandler(client, k, in, out, true);

				handlers.replace(k, newHandler);
				pool.execute(newHandler);
				unfinished.add(newHandler);
			} else if (!finished) { // Not finished, put back on the queue
				unfinished.add(curr);
			}
			else {
				//System.out.println("Worker appears to be finished and succcesful!");
			}
		}

		// Final clean up
		killProcs(reducerProcs);
		try {
			shutdownCarefully(pool);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		masterSocket.close();
	}

}
