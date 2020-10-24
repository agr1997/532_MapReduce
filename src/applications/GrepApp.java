package applications;

import java.io.*;
import java.nio.file.*;
import mapreduce.*;

/*
 * Given text annotated with line numbers, the lines
 * that have > 10 words.
 */
public class GrepApp {
	public static class GrepMapper extends Mapper {

		final int MIN_LEN = 11;

		// User-defined mapper
		@Override
		public void map(MapInput input) {
			//System.out.println("Heyo!");
			String line = null;
			try {
				line = input.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
			while(line != null) {
				// Skip over white space to next word
				String[] words = line.trim().split(" ");

				// If the line isn't long enough, continue.
				// First word is line number, not part of sentence.
				if (words.length < MIN_LEN + 1) continue;

				// Recreate sentence
				String lineNum = "";
				String sentence = "";
				for (int i=0; i < words.length; i++) {
					if (i == 0) {
						lineNum = words[i];
					}
					else {
						sentence += words[i];
					}
				}

				// Emit line
				try {
					emit(lineNum, sentence);
				} catch (IOException e) {
					e.printStackTrace();
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

	public static class GrepReducer extends Reducer {
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
				String valString = keyValList[1];

				// Emit
				try {
					emit(key, valString);
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
		String config_path = "./config/grep_cfg.txt";
		//String absolute_cfg = new File(config_path).getCanonicalPath();

		Path realPath = Paths.get(config_path).toRealPath(LinkOption.NOFOLLOW_LINKS);
		String absolute_path = realPath.toString();


		// Load specification from config file
		MapReduceSpecification spec = new MapReduceSpecification(absolute_path, "Grep");
//		System.out.printf("Spec has: N=%d ; input=%s ; output=%s ; name=%s \n",
//				spec.getN(), spec.getInput(), spec.getOutput(), spec.getName());

		MapReduce mr = new MapReduce(spec, GrepMapper.class, GrepReducer.class);

		int result = mr.execute();
	}

}
