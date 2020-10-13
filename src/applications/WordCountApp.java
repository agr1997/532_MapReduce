package applications;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;  

import mapreduce.MapInput;
import mapreduce.ReduceInput;
import mapreduce.MapReduce;
import mapreduce.MapReduceSpecification;
import mapreduce.Mapper;
import mapreduce.Reducer;

public class WordCountApp {
	public static class WordCounter extends Mapper{
		// UDF mapper
		//public WordCounter() {}
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
				
				for(String word : words) {
					try {
						emit(word, "1");
					} catch (IOException e) {
						// TODO Auto-generated catch block
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
				//String[] kv = line.trim().replaceAll("[()]", "").split(":-");
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
		// Need to get absolute path to pass to MapReduceSpecification, but will be different for each system we run on
		String config_path = "./config/wordcount_cfg.txt";
		//String absolute_cfg = new File(config_path).getCanonicalPath();
		
		Path realPath = Paths.get(config_path).toRealPath(LinkOption.NOFOLLOW_LINKS);
		String absolute_path = realPath.toString();
		
		
		
		// Load specification from config file		
		MapReduceSpecification spec = new MapReduceSpecification(absolute_path, " Word Count");
//		System.out.printf("Spec has: N=%d ; input=%s ; output=%s ; name=%s \n",
//				spec.getN(), spec.getInput(), spec.getOutput(), spec.getName());
		
		MapReduce mr = new MapReduce(spec, WordCounter.class, Adder.class);
		
		int result = mr.execute();
	}

}
