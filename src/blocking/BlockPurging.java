package blocking;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import general.CSVParser;

public class BlockPurging {
	
	
	
	/**
	 * This is a tokens-based block purging algorithm. It is constructed to take
	 * two files as input and treat them as CSV. It ignores their headers, and uses
	 * a tokenizer to convert them to bag of words. We used the tokenizer from
	 * our hadoop experiments.  We also use two thresholds to control skew. Small
	 * but crucial details include turning everything to lower-case before
	 * adding to instance sets. BE CAREFUL about this concerning IDs, since LEI
	 * IDs contain upper-case letters!
	 * 
	 */
	
	/*
	 * These two instances lists contain sets-of-words representations of instances
	 * from the two files that are assumed as input. We consider sets since this
	 * is only blocking. Note that IDs are not recorded herein.
	 */
	ArrayList<Set<String>> instances1;
	ArrayList<Set<String>> instances2;
	
	/*
	 * These contain the list of IDs in the two files, always assumed to be
	 * in the first column
	 */
	ArrayList<String> IDs1;
	ArrayList<String> IDs2;
	
	
	/*
	 * The tokenizer was borrowed from hadoop/src/census/TSG2
	 */
	static String[] tokenizer={"/", ",", ":", ";", "\\(", "\\)", "\\.", 
			"\"", "_", "-", "#", "\\\\", "\\s+"};
	
	/*
	 * These are the two thresholds for controlling skew.
	 */
	double blockThresh=10;
	double pairWiseThresh=20;
	
	static String rootFolder="C:\\Users\\Mayank\\SkyDrive\\Documents\\competitions\\feiii-2016\\feiii-data-20160202\\Data-and-Metadata\\homogenized\\";
	
	
	public static void main(String[] args){
		test_buildValidCommonKey();
	}
	
	/*
	 * We've designed all tests for FFIEC-SEC. For LEI, make sure to tweak accordingly,
	 * as IDs contain upper case letters.
	 */
	protected static void testConstructor(){
		BlockPurging obj=new BlockPurging(rootFolder+"FFIEC-homogenized.csv", 
				rootFolder+"SEC-homogenized.csv");
		System.out.println("instances1.size "+ obj.instances1.size());
		System.out.println("instances2.size "+obj.instances2.size());
		System.out.println("IDs1.size "+ obj.IDs1.size());
		System.out.println("IDs2.size "+obj.IDs2.size());
	}
	
	protected static void test_buildValidCommonKey(){
		BlockPurging obj=new BlockPurging(rootFolder+"FFIEC-homogenized.csv", 
				rootFolder+"SEC-homogenized.csv");
		Set<String> bkvs=obj.buildValidCommonKeySet();
		System.out.println("bkvs.size "+ bkvs.size());
		
	}
	
	
	/*
	 * The constructor is intended to populate the IDs and instances data structures.
	 */
	public BlockPurging(String file1, String file2){
		Scanner in=null;
		
		//read in first file
		try{
			//initialize
			
			in=new Scanner(new FileReader(file1));
			instances1=new ArrayList<Set<String>>();
			IDs1=new ArrayList<String>();
			
			//bypass header
			
			if(in.hasNextLine())
				in.nextLine();
			
			
			while(in.hasNextLine()){
				String line=in.nextLine().toLowerCase();
				String[] fields=(new CSVParser()).parseLine(line);
				
				//length check
				
				if(fields.length!=6){
					System.out.println("Error in BlockPurging! Field length != 6");
					System.out.println(line);
				}
				
				//add ID
				IDs1.add(fields[0]);
				
				//build set
				Set<String> tokens=new HashSet<String>();
				for(int i=1; i<fields.length; i++){
					for(String t: tokenizer)
						fields[i]=fields[i].replaceAll(t, " ").trim();
					String[] tmp=fields[i].split(" ");
					for(String t: tmp)
						tokens.add(t);
				}
				
				//add set to instances
				instances1.add(tokens);
			}
			
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			in.close();
		}
		
		//read in second file
		try{
			//initialize
			
			in=new Scanner(new FileReader(file2));
			instances2=new ArrayList<Set<String>>();
			IDs2=new ArrayList<String>();
			
			//bypass header
			
			if(in.hasNextLine())
				in.nextLine();
			
			
			while(in.hasNextLine()){
				String line=in.nextLine().toLowerCase();
				String[] fields=(new CSVParser()).parseLine(line);
				
				//length check
				
				if(fields.length!=6){
					System.out.println("Error in BlockPurging! Field length != 6");
					System.out.println(line);
				}
				
				//add ID
				IDs2.add(fields[0]);
				
				//build set
				Set<String> tokens=new HashSet<String>();
				for(int i=1; i<fields.length; i++){
					for(String t: tokenizer)
						fields[i]=fields[i].replaceAll(t, " ").trim();
					String[] tmp=fields[i].split(" ");
					for(String t: tmp)
						tokens.add(t);
				}
				
				//add set to instances
				instances2.add(tokens);
			}
			
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			in.close();
		}
		
	}
	/*
	 * This will return the set of BKVs (i.e. tokens) that are 
	 * (1) common to both instance sets, and
	 * (2) are valid, in terms of satisfying thresholds.
	 */
	public Set<String> buildValidCommonKeySet(){
		/*These are the six main data structures. smaller/larger will refer to the
		*smaller/larger of instances1 and 2 resp. output will be populated last.
		*{smaller/larger}Counts contain tuple counts in terms of tokens (like
		*document frequencies). Forbidden is a tmp structure that is used for
		*enforcing blockThresh constraints in both instance sets.
		*
		*/
		Set<String> output=new HashSet<String>();
		Map<String, Integer> smallerCounts=new HashMap<String,Integer>();
		Map<String, Integer> largerCounts=new HashMap<String,Integer>();
		Set<String> forbidden=new HashSet<String>();
		
		ArrayList<Set<String>> smaller =   
				instances1.size()<=instances2.size() ? instances1 : instances2;
		ArrayList<Set<String>> larger =   
				instances1.size()<=instances2.size() ? instances2 : instances1;
		if(smaller==larger)
			System.out.println("Error in BlockPurging.buildValidCommonKeySet()! smaller is same as larger!");
		
		
		
		 // We stop updating counts once we cross the threshold, and add to forbidden.
		 
		for(Set<String> instance: smaller)
			for(String token: instance){
				if(!smallerCounts.containsKey(token))
					smallerCounts.put(token, 0);
				int count=smallerCounts.get(token);
				if(count>=blockThresh)
					forbidden.add(token);
				else
					smallerCounts.put(token, count+1);
			}
		
		//enforce blockThresh constraint
		for(String token: forbidden)
			smallerCounts.remove(token);
		
		forbidden=null;
		forbidden=new HashSet<String>();
		
		for(Set<String> instance: larger)
			for(String token: instance){
				//a small optimization, to conserve space
				if(!smallerCounts.containsKey(token))
					continue;
				if(!largerCounts.containsKey(token))
					largerCounts.put(token, 0);
				int count=largerCounts.get(token);
				if(count>=blockThresh)
					forbidden.add(token);
				else
					largerCounts.put(token, count+1);
			}
		
		for(String token: forbidden)
			largerCounts.remove(token);
		
		//to conserve space.
		forbidden=null;
		
		/*enforce pairWiseThresh constraint and build final output set. It's best
		*to iterate over largerCounts since it's guaranteed to contain a subset of
		*smallerCounts keys 
		*/
		for(String bkv: largerCounts.keySet())
			
				if(smallerCounts.get(bkv)*largerCounts.get(bkv)<=pairWiseThresh)
					output.add(bkv);
		
	
		return output;
	}
	
	
	
	
}
