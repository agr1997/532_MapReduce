package mapreduce;

import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import com.google.code.externalsorting.ExternalSort;
import static java.nio.file.StandardOpenOption.*;
import java.nio.channels.FileChannel;

/**
 * Users define their reduce functions by creating a class which extends this
 * abstract Reducer class and implements the abstract function "reduce". The
 * rest of the code is necessary to carry out the work of a reducer.
 */
public abstract class Reducer {
	private int workerNumber;
	private BufferedWriter writer;
	private Path intermediateDir;
	private Path finalDir;
	private Path mergeSortFilePath;
	private Path finalFilePath;
	private String currKey;
	private static final int workerSocketTimeout = 2000; // In milliseconds
	private static final int PORT = 9002;

	public abstract void reduce(ReduceInput input) throws IOException; // Implemented by the user

	/**
	 * Obtains the intermediate files produced by the mappers and sorts them by key
	 * using an external sorting library from Google.
	 */
	public void mergeAndSort(String[] intermediateFilePaths) throws IOException {
		Path mergePath = this.intermediateDir.resolve("merge-reducer-" + workerNumber);
		this.mergeSortFilePath = this.intermediateDir.resolve("sorted-reducer-" + workerNumber);

		// Merge the intermediate files into one file which can then be sorted
		try (FileChannel out = FileChannel.open(mergePath, CREATE, WRITE)) {
			for (String path : intermediateFilePaths) {
				try (FileChannel in = FileChannel.open(Paths.get(path), READ)) {
					for (long p = 0, l = in.size(); p < l;)
						p += in.transferTo(p, l - p, out);
				}
			}
		}

		// Sort the merged file using external sort library
		ExternalSort.mergeSortedFiles(ExternalSort.sortInBatch(new File(mergePath.toString())),
				new File(this.mergeSortFilePath.toString()));
	}

	/**
	 * Carries out the work of a given reducer worker, which receives and sorts the
	 * intermediate files from the mappers and invokes the user-defined reduce
	 * function on those intermediate results.
	 */
	public void execute(String[] intermediateFilePaths) throws IOException {
		this.setFinalFilePath();
		this.setMergeSortFilePath();
		mergeAndSort(intermediateFilePaths);
		long inputSize = Files.size(this.mergeSortFilePath);

		// Set up necessary readers and writers
		FileInputStream fileStream = null;
		try {
			fileStream = new FileInputStream(this.mergeSortFilePath.toString());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		InputStreamReader inputReader = new InputStreamReader(fileStream);
		BufferedReader reader = new BufferedReader(inputReader);

		/* Create one ReduceInput for each new key observed, and call
		 * the user-defined reduce function on that ReduceInput object.
		 */
		this.setupWriter();
		ReduceInput currInput = new ReduceInput(reader, inputSize);
		while (!currInput.eof()) {
			this.currKey = currInput.getKey();
			this.reduce(currInput);
			reader = currInput.getReader();
			currInput = new ReduceInput(reader, inputSize);
		}

		reader.close();
		this.closeWriter();
	}

	/**
	 * Given a line that was produced by a mapper, returns a length-2 String array
	 * containing the key in the first location and the value in the second.
	 * 
	 * @param line - a line produced by a mapper
	 * @return - the (key,value) pair as a String array
	 * @throws IOException
	 */
	public static String[] processMapperLine(String line) throws IOException {
		line = line.trim();
		// Get substring between parentheses and split on special separator
		String[] kv = line.substring(1, line.length() - 1).split("%&%");
		if (kv.length != 2) {
			System.err.println("Received ill-formatted line from mapper.");
			throw new IOException();
		}
		return kv;
	}

	/**
	 * The user-defined reducer method will invoke this function once to emit a
	 * (key,value) pair for a given key. Reducer worker already knows current key
	 * being processed.
	 * 
	 * @param value - value the user wants to emit
	 * @throws IOException
	 */
	public void emit(String value) throws IOException {
		String output = "(" + this.currKey + "%&%" + value + ")" + "\n";
		this.writer.write(output);
	}

	// setter for workerNumber
	public void setWorkerNumber(int k) {
		this.workerNumber = k;
	}

	// setter for intermediate Dir
	public void setIntermediate(Path intermediateDir) {
		this.intermediateDir = intermediateDir;
	}

	// setter for finalDir
	public void setFinal(Path finalDir) {
		this.finalDir = finalDir;
	}

	// setter for finalFilePath
	public void setFinalFilePath() {
		Path dir = finalDir;
		String reducerExtension = "reducer-" + this.workerNumber;
		this.finalFilePath = dir.resolve(reducerExtension);
	}

	// setter for mergeSortFilePath
	public void setMergeSortFilePath() {
		Path dir = intermediateDir;
		String reducerExtension = "reducer-" + this.workerNumber;
		this.mergeSortFilePath = dir.resolve(reducerExtension);
	}

	// getter for finalFilePath
	public Path getFinalFilePath() {
		return this.finalFilePath;
	}

	/**
	 * Set up the writer this reducer worker will use to write out the final
	 * results.
	 */
	public void setupWriter() {
		try {
			this.writer = new BufferedWriter(new FileWriter(this.finalFilePath.toString()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// Close all writers after reducer tasks are complete
	public void closeWriter() {
		try {
			this.writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Invoked to spawn a new reducer worker process and facilitate its work.
	 * 
	 * @param args - arguments specifying the reducer's work
	 * @exception IOException Upon error involving the socket.
	 */
	public static void main(String[] args) throws IOException {
		Socket clientSocket = new Socket("localhost", PORT);
		clientSocket.setSoTimeout(workerSocketTimeout);

		// Parse args received, which specify reducer's work
		String udfReducerClassName = args[0];
		int workerNum = Integer.parseInt(args[1]);
		Path intermediateDirName = Paths.get(args[2]);
		Path finalDir = Paths.get(args[3]);
		int sleepLength = Integer.parseInt(args[4]);

		// Log the worker's process ID, for testing purposes
		RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
		long pid = Long.valueOf(bean.getName().split("@")[0]);
		System.out.printf("Reducer:%d\nPID=%d\n", workerNum, pid);

		BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

		// Receive intermediate file paths from the master
		String[] intermediateFilePaths = null;
		try {
			while (true) {
				System.out.println("Waiting for filepaths.\n");
				String serverMsg = in.readLine();

				if (serverMsg == null) {
					System.err.println("Socket closed server-side.");
					throw new IOException();
				} else if (serverMsg.contains("Filepaths")) {
					intermediateFilePaths = in.readLine().split("\t");
					System.out.println("Received filepaths.\n");
					break;
				}
			}
		} catch (SocketException | SocketTimeoutException e) {
			System.err.printf("Reducer timed out waiting for file paths from master.\n");
			System.exit(1);
		} catch (IOException e) {
			System.exit(1);
		}
		// Acknowledge to master that file paths were received
		out.println("received");

		// In a separate thread, reply to heartbeats from the master
		System.err.println("Starting pinger.");
		Thread pinger = new Thread(new ServerConnection(clientSocket, in, out));
		pinger.start();

		// Instantiate UDF reducer
		Reducer r = null;
		try {
			Class udfReducerClass = Class.forName(udfReducerClassName);
			try {
				r = (Reducer) udfReducerClass.getConstructor().newInstance();

				r.setWorkerNumber(workerNum); // Give mapper its worker number
				r.setIntermediate(intermediateDirName);
				r.setFinal(finalDir);

			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException | SecurityException e) {
				e.printStackTrace();
			}
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}

		// Execute reduce work
		r.execute(intermediateFilePaths);

		/*
		 * Used in case of testing fault tolerance, to ensure this process gets killed
		 * before successful completion of its work.
		 */
		if (sleepLength > 0) {
			try {
				Thread.sleep(sleepLength);
			} catch (InterruptedException e2) {
				e2.printStackTrace();
			} finally {
				System.exit(1);
			}
		}

		// Shut down pinger in preparation for final write to 'out'
		pinger.interrupt();
		try {
			pinger.join();
		} catch (InterruptedException e) {
			// Current thread was interrupted while waiting for pinger to join
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}

		// Inform master that the work is complete, and wait for ack
		out.println("Finished");
		try {
			while (true) {
				// Might need to read a leftover 'ping' first
				String line = in.readLine();

				if (line == null) {
					System.err.println("Socket closed server-side.");
					throw new IOException();
				} else if (line.contains("received")) {
					break;
				}
			}
		} catch (SocketException | SocketTimeoutException e) {
			System.err.printf("Reducer timed out waiting to hear that master heard that it had finished.\n");
			System.exit(1);
		}
		out.close();
		clientSocket.close();
	}
}
