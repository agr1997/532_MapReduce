package applications;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import mapreduce.*;

/**
 * An application which takes in a text file annotated with line numbers
 * and outputs an index mapping each word to the list of lines 
 * in which it appears.
 */
public class InvertedIndexApp {
	public static class InvertedIndexMapper extends Mapper {

		// User-defined mapper for the InvertedIndex function
		@Override
		public void map(MapInput input) throws IOException {

			String line = null;
			line = input.readLine();
			while (line != null) {
				// Skip over white space to next word
				String[] words = line.trim().split(" ");

				// The first token is the line number
				String lineNum = words[0];

				// The remaining tokens are the words on that line
				for (int i = 1; i < words.length; i++) {
					emit(words[i], lineNum);
				}

				// Read next line
				line = input.readLine();
			}
		}
	}

	public static class InvertedIndexReducer extends Reducer {

		// UDF reducer
		@Override
		public void reduce(ReduceInput input) throws IOException {

			ArrayList<Integer> values = new ArrayList<Integer>();

			while (!input.done()) {
				String value = input.value();
				values.add(Integer.valueOf(value));
				input.nextValue();
			}
			int[] valuesArray = new int[values.size()];
			for (int i = 0; i < values.size(); i++) {
				valuesArray[i] = values.get(i);
			}
			Arrays.sort(valuesArray);

			// Format into one string
			String sortedValString = "[";
			for (int val : valuesArray) {
				sortedValString += String.valueOf(val) + ", ";
			}
			int stringLength = sortedValString.length();
			sortedValString = sortedValString.substring(0, stringLength - 2) + "]";

			emit(sortedValString);

		}
	}

	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			System.out.println("Write the name of a config file on the command line.");
		}
		String cfgRelPath = "./config/" + args[0];
		String cfgAbsPath = Paths.get(cfgRelPath).toRealPath(LinkOption.NOFOLLOW_LINKS).toString();

		// Load specification from config file
		MapReduceSpecification spec = new MapReduceSpecification(cfgAbsPath, "Inverted Index");

		MapReduce mr = new MapReduce(spec, InvertedIndexMapper.class, InvertedIndexReducer.class);

		mr.execute(args);
	}

}
