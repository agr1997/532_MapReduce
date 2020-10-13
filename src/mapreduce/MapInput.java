package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;

public class MapInput {
	private BufferedReader reader;
	private int numCharsToRead;
	private int numRead;
	
	public MapInput(BufferedReader reader, int numCharsToRead) {
		// We are assuming reader is beginning at correct byte
		this.reader = reader;
		this.numCharsToRead = numCharsToRead;
		this.numRead = 0;
	}
	
	public String readLine() throws IOException {
		String line = reader.readLine();
		if(line != null && numRead < numCharsToRead) {
			int length = line.length();
			this.numRead += (length + 1); // +1 to count the newline character at end of 'line'
			return line;
		}
		else return null;
	}

}
