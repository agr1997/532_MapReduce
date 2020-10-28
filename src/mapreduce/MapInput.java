package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * A class which wraps around a BufferedReader so that the user-defined function
 * can treat the resulting object like a standard file reader and yet the mapper
 * worker knows where to start and stop reading the file, based on the adjusted
 * offsets determined inside Mapper.java's execute() function.
 */
public class MapInput {
	private BufferedReader reader;
	private int numCharsToRead;
	private int numRead;

	/**
	 * Constructor.
	 * 
	 * @param reader         - a reader for the input file
	 * @param numCharsToRead - a limit on the number characters read that this
	 *                       wrapper shallow allow to be read from the reader
	 */
	public MapInput(BufferedReader reader, int numCharsToRead) {
		// We are assuming reader is already at correct start byte
		this.reader = reader;
		this.numCharsToRead = numCharsToRead;
		this.numRead = 0;
	}

	/**
	 * Attempts to read a line from the input file, returning 'null' in case the
	 * reader has reached the end of the file or the reader has read as many chars
	 * as it was designed to.
	 * 
	 * @return - the line that is read from the input file, or 'null'
	 * @throws IOException
	 */
	public String readLine() throws IOException {
		String line = reader.readLine();
		if (line != null && numRead < numCharsToRead) {
			int length = line.length();
			// +1 to count the newline character at end of 'line'
			this.numRead += (length + 1);
			return line;
		} else
			return null;
	}

}
