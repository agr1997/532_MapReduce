package applications;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import mapreduce.*;

/*
 * Given text annotated with line numbers, return an index
 * mapping each word to the lines it appears in.
 */
public class InvertedIndexApp {
	public static class InvertedIndexMapper extends Mapper {

		// User-defined mapper for the InvertedIndex function
		@Override
		public void map(MapInput input) {

			String line = null;
			try {
				line = input.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
			while(line != null) {
				// Skip over white space to next word
				String[] words = line.trim().split(" ");

				// Convert to list of Strings
				ArrayList<String> wordsList = (ArrayList<String>)Arrays.asList(words);

				// The first token is the line number
				String lineNum = wordsList.remove(0);

				// The remaining tokens are the words on that line
				for(String word : wordsList) {
					try {
						emit(word, lineNum);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				// Read next line
				try {
					line = input.readLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public static class InvertedIndexReducer extends Reducer {

		// UDF reducer
		@Override
		public void reduce(ReduceInput input) {
			String line = null;
			try {
				line = input.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}

			while(line != null) {
				// Skip over white space to next word
				line = line.trim();

				// Extract key and value-list from line
				String[] keyValList = line.substring(1,line.length()-1).split("%&%");
				String key = keyValList[0];
				String[] valListString = keyValList[1].split(" ");

				// Convert String array to int array
				int[] valListInt = new int[valListString.length];
				for (int i=0; i < valListString.length; i++) {
					valListInt[i] = Integer.valueOf(valListString[i]);
				}
				// Sort the int array
				Arrays.sort(valListInt);

				// Convert value-list to String in desired print format
				String sortedValString = "[";
				for ( int val : valListInt) {
					sortedValString += String.valueOf(val) + ", ";
				}
				int stringLength = sortedValString.length();
				sortedValString = sortedValString.substring(0, stringLength - 2) + "]";

				// Emit
				try {
					emit(key, sortedValString);
				} catch (IOException e) {
					e.printStackTrace();
				}

				// Read next line
				try {
					line = input.readLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
	}


	public static void main(String[] args) throws IOException {
		// Need to get absolute path to pass to MapReduceSpecification, but will be different for each system we run on
		String config_path = "./config/invertedindex_cfg.txt";
		//String absolute_cfg = new File(config_path).getCanonicalPath();

		Path realPath = Paths.get(config_path).toRealPath(LinkOption.NOFOLLOW_LINKS);
		String absolute_path = realPath.toString();


		// Load specification from config file
		MapReduceSpecification spec = new MapReduceSpecification(absolute_path, "Inverted Index");
//		System.out.printf("Spec has: N=%d ; input=%s ; output=%s ; name=%s \n",
//				spec.getN(), spec.getInput(), spec.getOutput(), spec.getName());

		MapReduce mr = new MapReduce(spec, InvertedIndexMapper.class, InvertedIndexReducer.class);

		int result = mr.execute();
	}

}
