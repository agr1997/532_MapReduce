package applications;

import java.io.IOException;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.*;  

import mapreduce.MapInput;
import mapreduce.ReduceInput;
import mapreduce.MapReduce;
import mapreduce.MapReduceSpecification;
import mapreduce.Mapper;
import mapreduce.Reducer;

/* 
 * A simple application for reading in a text file
 * and obtaining key-value pairs which consist of the
 * unique words in the text file and their counts.
 */
public class WordCountApp {
	public static class WordCounter extends Mapper{
		// User-defined mapper for the WordCounter function
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
				
				for(String word : words) {
					try {
						emit(word, "1");
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				try {
					line = input.readLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			

		}
	}
	
	public static class Adder extends Reducer{
		// UDF reducer
		@Override
		public void reduce(ReduceInput input) {
			String line = null;
			try {
				line = input.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
			HashMap<String, Integer> map = new HashMap<String, Integer>();  
			while(line != null) {
				// Skip over white space to next word
				line = line.trim();
				String[] kv = line.substring(1,line.length()-1).split("%&%");
				
				if (map.containsKey(kv[0])) {
					map.replace(kv[0], map.get(kv[0]) + Integer.valueOf(kv[1]));
				}
				else {
					map.put(kv[0], Integer.valueOf(kv[1]));
				}
				try {
					line = input.readLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}	
			
		    for (String key : map.keySet()) {
		    	try {
		    		emit(key, Integer.toString(map.get(key)));
		    	} catch (Exception e) {
		    		e.printStackTrace();
		    	}
		    }
		}
	}
	
	
	public static void main(String[] args) throws IOException {
		String cfgRelPath = "./config/wordcount_cfg.txt";
		
		String cfgAbsPath = Paths.get(cfgRelPath).toRealPath(LinkOption.NOFOLLOW_LINKS).toString();
		
		// Load specification from config file		
		MapReduceSpecification spec = new MapReduceSpecification(cfgAbsPath, " Word Count");
		
		MapReduce mr = new MapReduce(spec, WordCounter.class, Adder.class);
		
		int result = mr.execute();
		System.exit(0);
	}

}
