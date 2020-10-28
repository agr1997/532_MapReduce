package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * A wrapper around a BufferedReader which is reading in the sorted, merged file
 * of (k,v) pairs for a given reducer worker. One ReduceInput object is to be
 * created for each new key seen by the underlying reader.
 */
public class ReduceInput {
	private BufferedReader reader;
	private int inputSize;
	private boolean eof = false; // For use inside Reducer execute() function
	private boolean done = false; // For use in the user-defined reduce
	private String key; // The key associated with this ReduceInput
	private String currValue; // Most recent value seen for this key

	/**
	 * Constructor.
	 * 
	 * @param reader    - A reader for sorted, merged (k,v) pair file for a reducer
	 *                  worker
	 * @param inputSize - Size of merged file, for marking purposes (maybe
	 *                  pointless)
	 */
	public ReduceInput(BufferedReader reader, long inputSize) throws IOException {
		this.reader = reader;
		this.inputSize = Math.toIntExact(this.inputSize);

		// What key is this input handling?
		String line = reader.readLine();
		if (line == null) {
			/*
			 * Underlying reader reached eof, so we should stop this ReduceInput before it
			 * does any work.
			 */
			this.eof = true;
			this.done = true;
		} else {
			String[] kv = Reducer.processMapperLine(line);
			this.key = kv[0];
			this.currValue = kv[1];
		}
	}

	/**
	 * Called from the user-defined reduce function to process the next value for
	 * the current key, if possible. If this ReduceInput has already finished
	 * processing the values for a given key, this.eof and/or this.done will be set
	 * to 'true', and calls to this function change nothing.
	 * 
	 * @throws IOException
	 */
	public void nextValue() throws IOException {
		if (this.eof || this.done) {
			System.err.println("No more values to " + "read from ReduceInput object.");
			return;
		}

		reader.mark(1024);
		String line = reader.readLine();
		if (line == null) {
			// Underlying reader reached end of file
			this.eof = true;
			this.done = true;
		} else {
			String[] kv = Reducer.processMapperLine(line);
			// Case that we are still processing lines with same key
			if (this.key.equals(kv[0])) {
				this.currValue = kv[1];
			}
			/*
			 * Otherwise should reset to mark before passing reader off to the next
			 * ReduceInput object.
			 */
			else {
				this.currValue = null; // To avoid silent errors in user's code
				this.done = true;
				// A different ReduceInput object should read this line
				try {
					reader.reset();
				} catch (IOException e) {
					System.err.printf("Tried to reset and failed\n" + "Line is: %s\n", line);
					throw new IOException(e);
				}
			}
		}
	}

	// Getter for key
	public String getKey() {
		return this.key;
	}

	// Getter for currValue
	public String value() {
		return currValue;
	}

	// Getter for done
	public boolean done() {
		return this.done;
	}

	// Getter for eof
	public boolean eof() {
		return this.eof;
	}
	
	// Getter for reader
	public BufferedReader getReader() {
		return this.reader;
	}
}