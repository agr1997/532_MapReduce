package mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Users define their map functions by creating a class which extends this
 * abstract Mapper class and implements the abstract function "map". The rest of
 * the code in this class facilitates the work of a mapper worker.
 */
public abstract class Mapper {
	private int numWorkers;
	private BufferedWriter[] writers;
	private Path[] intermediateFilePaths;
	private long inputSize;
	private Path inputFilePath;
	private Path intermediateDir;
	private int workerNumber;
	private static final int workerSocketTimeout = 500; // In milliseconds
	private static final int PORT = 9002;

	public abstract void map(MapInput input) throws IOException; // Implemented by the user

	/**
	 * Carries out the work of a single mapper worker, which involves finding
	 * appropriate byte ranges of the input file to read and creating a
	 * corresponding MapInput object, and finally invoking the user-defined "map"
	 * function.
	 */
	public void execute() throws IOException {
		int inputSize = Math.toIntExact(this.inputSize);

		// Coarse byte offsets based solely on worker number
		int start = this.workerNumber * inputSize / this.numWorkers;
		int end = Math.min((this.workerNumber + 1) * inputSize / this.numWorkers, inputSize - 1);
		if (this.workerNumber == this.numWorkers - 1) {
			end = inputSize - 1;
		}
		System.err.printf(String.format("Mapper: %d \t Start: %d \t End: %d\n", this.workerNumber, start, end));
		int windowWidth = end - start + 1;
		System.err.printf("Worker: %d\tStart: %d\tEnd: %d\tWindow Width: %d\n", this.workerNumber, start, end,
				windowWidth);
		// Note: in the case that n=1, then end= 1 * inputSize / 1 = inputSize
		// So in total should read from byte 0=start up to and including byte
		// (inputSize-1)=end
		// and a given worker will reader from as early as the (start+1)-th byte up to
		// and including the end-th

		FileInputStream fileStream = null;
		try {
			fileStream = new FileInputStream(this.inputFilePath.toString());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		InputStreamReader inputReader = new InputStreamReader(fileStream);
		BufferedReader reader = new BufferedReader(inputReader);

		// Adjust start byte
		reader.skip(start); // The first byte we read will be the (start)-th, counting up from zero
		reader.mark(inputSize);
		if (start != 0) {
			// Find the next newline starting from 'start'
			try {
				String line = reader.readLine();
				if (line == null || (line.length() >= windowWidth)) {
					throw new IOException();
				}
				int i1 = line.length(); // Length of readLine - (eol/cr/nl) from start
				System.err.printf("About to skip, end is:[%d] start is: [%d]: i1 is [%d].", start, end, i1);
				reader.mark(inputSize);
				reader.skip(end - (start + i1 + 1)); // Now skip to the coarse byte boundary for 'end'
				start = start + i1 + 1;
				System.err.printf("Sta Worker: %d\ti1: %d\n", this.workerNumber, i1);
			} catch (IOException e) { // TODO IOException is not really the most precise exception to use here
				System.out.printf("Worker %d reached 'end' " + "without seeing a newline while adjusting 'start'.\n",
						this.workerNumber);
				e.printStackTrace();
			}
		} else {
			reader.skip(end);
		}

		// Adjust end byte
		if (end != inputSize - 1) {
			try {
				String line = reader.readLine();
				if (line == null || (line.length() >= windowWidth)) {
					throw new IOException();
				}
				int i1 = line.length();
				end = end + i1;
				System.err.printf("End Worker: %d\ti1: %d\n", this.workerNumber, i1);
			} catch (IOException e) {
				System.out.printf("Worker %d pasqsed worker %d's 'end' without seeing a newline "
						+ "while adjusting its own 'end'.\n", this.workerNumber, this.workerNumber + 1);
				e.printStackTrace();
			}
		}

		reader.reset(); // Should take us back to the updated starting point for this chunk
		int numCharsToRead = end - start + 1;
		System.err.printf("Worker: %d\tStart: %d\tEnd: %d\tnumCharsToRead: %d\n", this.workerNumber, start, end,
				numCharsToRead);
		MapInput mapInput = new MapInput(reader, numCharsToRead);

		// Set up file paths, writers, and call the user-defined map function.
		this.setupIntermediateFilePaths();
		this.setupWriters();
		this.map(mapInput);
		this.closeAllWriters();
		reader.close();
	}

	/**
	 * Called from user's implementation of the map method to write out a given key,
	 * value pair. Hashing used to determine appropriate buffer for the intermediate
	 * file for the reducer which will be handling that key.
	 * 
	 * @param key
	 * @param value
	 * @throws IOException
	 */
	public void emit(String key, String value) throws IOException {
		// Delete any instances of our special separator
		key = key.replaceAll("%&%", "");
		value = value.replaceAll("%&%", "");

		int h = Math.abs(key.hashCode()) % this.numWorkers;
		String output = "(" + key + "%&%" + value + ")" + "\n";
		BufferedWriter writer = this.writers[h];
		writer.write(output);
	}
	
	// Setter for numWorker
	public void setNumWorkers(int n) {
		this.numWorkers = n;
	}
	
	// Setter for inputSize
	public void setInputSize(long size) {
		this.inputSize = size;
	}

	// Setter for inputFilePath
	public void setInputFilePath(Path inputFilePath) {
		this.inputFilePath = inputFilePath;
	}

	// Setter for k
	public void setWorkerNumber(int k) {
		this.workerNumber = k;
	}

	// Setter for intermediateDir
	public void setIntermediateDir(Path intermediateDir) {
		this.intermediateDir = intermediateDir;
	}

	/**
	 * Sets a private instance variable which is a Path array of paths to the
	 * intermediate files, using the worker number and N to determine the
	 * appropriate names for the files for this mapper worker.
	 */
	private void setupIntermediateFilePaths() {
		Path dir = intermediateDir;
		this.intermediateFilePaths = new Path[this.numWorkers];
		for (int i = 0; i < this.numWorkers; i++) {
			String workerExtension = "mapper" + "-" + this.workerNumber + "-" + i;
			this.intermediateFilePaths[i] = dir.resolve(workerExtension);
		}
	}

	/**
	 * Returns a string array of paths to the intermediate files, not the Path
	 * objects themselves, for the purpose of sending to the master process after
	 * completion of mapping work.
	 * 
	 * @return the desired array of file paths
	 */
	private String[] getIntermediateFilePaths() {
		String[] paths = new String[this.numWorkers];
		for (int i = 0; i < this.numWorkers; i++) {
			paths[i] = this.intermediateFilePaths[i].toString();
		}
		return paths;
	}

	/**
	 * Sets up an array of BufferedWriters, one for each of the N reducers which we
	 * are producing an intermediate file for. Must be called after
	 * setupIntermediateFilePaths().
	 */
	private void setupWriters() {
		this.writers = new BufferedWriter[this.numWorkers];
		for (int i = 0; i < this.numWorkers; i++) {
			String currFile = this.intermediateFilePaths[i].toString();
			try {
				this.writers[i] = new BufferedWriter(new FileWriter(currFile));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	// Flushes and closes the N BufferedWrites after ensuring they are flushed.
	public void closeAllWriters() {
		for (int i = 0; i < this.numWorkers; i++) {
			try {
				this.writers[i].flush();
				this.writers[i].close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Invoked to spawn a new mapper worker process and facilitate its work.
	 * 
	 * @param args - arguments specifying the mapper's work
	 * @exception IOException Upon error involving the socket.
	 */
	public static void main(String[] args) throws IOException {
		Socket clientSocket = new Socket("localhost", PORT);
		clientSocket.setSoTimeout(workerSocketTimeout);

		// Parse args received, which specify this mapper's work
		String udfMapperClassName = args[0];
		int numWorkers = Integer.parseInt(args[1]);
		int workerNum = Integer.parseInt(args[2]);
		long inputSize = Long.parseLong(args[3]);
		Path intermediateDirName = Paths.get(args[4]);
		Path inputFilePath = Paths.get(args[5]);
		int sleepLength = Integer.parseInt(args[6]);

		// Log the PID of the process associated with this worker's JVM
		RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
		long pid = Long.valueOf(bean.getName().split("@")[0]);
		System.out.printf("Mapper:%d\nPID=%d\n", workerNum, pid);

		BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

		// In a separate thread, reply to heartbeats from the master
		Thread pinger = new Thread(new ServerConnection(clientSocket, in, out));
		pinger.start();

		// Use the reflection library to instantiate the user-defined mapper
		Mapper m = null;
		try {
			Class udfMapperClass = Class.forName(udfMapperClassName);
			try {
				m = (Mapper) udfMapperClass.getConstructor().newInstance();

				m.setNumWorkers(numWorkers); // Give mapper the # of workers
				m.setWorkerNumber(workerNum); // Give mapper its worker number
				m.setInputSize(inputSize); // Give mapper the file size
				m.setIntermediateDir(intermediateDirName);
				m.setInputFilePath(inputFilePath);

			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException | SecurityException e) {
				e.printStackTrace();
				System.exit(1);
			}
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
			System.exit(1);
		}

		// Execute this mapper worker's work
		m.execute();
		
		/* Used in case of testing fault tolerance, to ensure this process
		 * gets killed before successful completion of its work.
		 */
		if (sleepLength > 0) {
			try {
				// Wait for a kill command
				Thread.sleep(sleepLength);
			} catch (InterruptedException e2) {
				// Do nothing, not concerned with interruption here
			}
			finally {
				// If no kill command received, kill self
				System.exit(1);
			}
		}

		// Shut down pinger in preparation for final write to 'out'
		pinger.interrupt();
		try {
			pinger.join();
		} catch (InterruptedException e) {
			// The current thread was interrupted while waiting for join
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}

		// Convey intermediate file paths to master and then wait for response
		String[] paths = m.getIntermediateFilePaths();
		String fp = "Filepaths\n" + String.join("\t", paths);
		out.println(fp);
		try {
			while (true) {
				String line = in.readLine();

				if(line == null) {
					System.err.println("Socket closed server-side.");
					throw new IOException();
				}
				else if(line.contains("received")){
					break;
				}
			}
		} catch (SocketException | SocketTimeoutException e) {
			System.err.printf("Mapper timed out waiting to hear that master" + "had received filepaths.\n");
			System.exit(1);
		}
		catch (IOException e) {
			System.exit(1);
		}
		// With knowledge of receipt, now safe to close the socket
		out.close();
		clientSocket.close();
	}

}
