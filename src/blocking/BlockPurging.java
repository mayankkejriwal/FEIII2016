package blocking;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import general.CSVParser;
import matching.JaccardMatcher;

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
	int blockThresh=Integer.MAX_VALUE;
	int pairWiseThresh=3000;
	
	static String rootFolder="C:\\Users\\Mayank\\SkyDrive\\Documents\\competitions\\feiii-2016\\feiii-data-20160202\\Data-and-Metadata\\homogenized\\";
	
	
	public static void main(String[] args){
		
		JaccardMatcher.submissionFFIECLEI(rootFolder+"FFIEC-homogenized.csv", 
				rootFolder+"LEI-homogenized.csv", 
				rootFolder+"submissions\\");
	
		//test_printPairsCompleteness();
		//test_printReductionRatio();
	}
	
	protected static void test_writtenOutBilateralBlocks(){
		Scanner in=null;
		int numPairs=0;
		try{
			in=new Scanner(new FileReader(rootFolder+"FFIEC-SEC-bilateralBlocks"));
			while(in.hasNextLine()){
				String line=in.nextLine();
				numPairs+=line.split("\t").length-1;
			}
		}catch(IOException e){
			e.printStackTrace();
		}finally{in.close();}
		System.out.println("Number of pairs in written out BilateralBlocks file is "+numPairs);
	}
	
	/*
	 * This is not a test file.
	 */
	protected static void writeOutBilateralBlocks(){
		BlockPurging obj=new BlockPurging(rootFolder+"FFIEC-homogenized.csv", 
				rootFolder+"SEC-homogenized.csv");
		obj.writeBilateralBlocksToFile(obj.buildValidCommonKeySet(),
				rootFolder+"FFIEC-SEC-bilateralBlocks");
	}

	/*
	 * We've designed all tests for FFIEC-SEC, but if everything's good, should also
	 * work for LEI.
	 */
	protected static void testConstructor(){
		BlockPurging obj=new BlockPurging(rootFolder+"FFIEC-homogenized.csv", 
				rootFolder+"SEC-homogenized.csv");
		System.out.println("instances1.size "+ obj.instances1.size());
		System.out.println("instances2.size "+obj.instances2.size());
		System.out.println("IDs1.size "+ obj.IDs1.size());
		System.out.println("IDs2.size "+obj.IDs2.size());
	}
	
	/*
	 * The test is not rigorous. We are only checking to see if it works, and
	 * whether the numbers seem to look right.
	 */
	protected static void test_buildValidCommonKey(){
		BlockPurging obj=new BlockPurging(rootFolder+"FFIEC-homogenized.csv", 
				rootFolder+"SEC-homogenized.csv");
		Set<String> bkvs=obj.buildValidCommonKeySet();
		System.out.println("bkvs.size "+ bkvs.size());
		
	}
	
	/*
	 * The test is not rigorous. We are only checking to see if it works, and
	 * whether the numbers seem to look right.
	 */
	protected static void test_printReductionRatio(){
		Set<Integer> fieldsToPopulate=new HashSet<Integer>();
		int[] fields={1};
		for(int field:fields)
			fieldsToPopulate.add(field);
		
		BlockPurging obj=new BlockPurging(rootFolder+"FFIEC-homogenized.csv", 
				rootFolder+"SEC-homogenized.csv", fieldsToPopulate);
		Set<String> bkvs=obj.buildValidCommonKeySet();
		obj.printReductionRatio(bkvs);
		
	}
	
	/*
	 * The test is not rigorous. We are only checking to see if it works, and
	 * whether the numbers seem to look right.
	 */
	protected static void test_printPairsCompleteness(){
		Set<Integer> fieldsToPopulate=new HashSet<Integer>();
		int[] fields={1};
		for(int field:fields)
			fieldsToPopulate.add(field);
		
		BlockPurging obj=new BlockPurging(rootFolder+"FFIEC-homogenized.csv", 
				rootFolder+"SEC-homogenized.csv", fieldsToPopulate);
		Set<String> bkvs=obj.buildValidCommonKeySet();
		obj.printPairsCompleteness(bkvs, 
				rootFolder+"ffiec-sec-partial-ground-truth.csv");
		
	}
	
	
	/*
	 * This method takes an arraylist of strings and returns an inverted index.
	 * Although we've made this static since we'll be accessing this in later
	 * packages and don't want to make the fields in this class public, it
	 * is intended for use with IDs1 and IDs2.
	 */
	public static Map<String, Integer> buildIDMap(ArrayList<String> IDs){
		Map<String, Integer> IDMap = new HashMap<String, Integer>();
		int count=0;
		for(String id: IDs){
			if(IDMap.containsKey(id))
				System.out.println("Warning in buildIDMap. ID "+id+" occurs more than once");
		
			IDMap.put(id, count);
			count++;
		}
			
		
		return IDMap;
	}

	/*
	 * This function takes in the goldStandardFile (see caveat in printPairsCompleteness)
	 * and returns a map, where the key is an id1 that references a set of id2s that
	 * together represent matching pairs in the goldStandardFile.
	 * If pos is true, we will only return the set with 'yes' instances, otherwise
	 * we return the set with 'no' instances
	 */
	public static Map<String, Set<String>> buildGoldSet(String goldStandardFile, boolean pos){
		
		Map<String, Set<String>> goldSet=new HashMap<String, Set<String>>();
		Scanner in=null;
		try{
			in=new Scanner(new FileReader(goldStandardFile));
			if(in.hasNextLine())	//header; ignore it
				in.nextLine();
			while(in.hasNextLine()){
				String line=in.nextLine();
				String[] fields=(new CSVParser()).parseLine(line);
				if(pos){
					if(fields[2].toLowerCase().equals("yes")){
						if(!goldSet.containsKey(fields[0]))
							goldSet.put(fields[0], new HashSet<String>());
						goldSet.get(fields[0]).add(fields[1]);
						
					}else if(!fields[2].toLowerCase().equals("no"))
						System.out.println("Error in buildGoldSet. line: "+line);
				}else{
					if(fields[2].toLowerCase().equals("no")){
						if(!goldSet.containsKey(fields[0]))
							goldSet.put(fields[0], new HashSet<String>());
						goldSet.get(fields[0]).add(fields[1]);
						
					}else if(!fields[2].toLowerCase().equals("yes"))
						System.out.println("Error in buildGoldSet. line: "+line);
				}
			}
			
		}catch(IOException e){
			e.printStackTrace();
		}
		finally{
			in.close();
		}
		
		return goldSet;
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
				String line=in.nextLine();
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
						tokens.add(t.toLowerCase());
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
				String line=in.nextLine();
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
						tokens.add(t.toLowerCase());
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
	 * The constructor is intended to populate the IDs and instances data structures,
	 * but only certain fields will be considered. The indices of these fields must
	 * be included in the Set data structure, which cannot be null.
	 */
	public BlockPurging(String file1, String file2, Set<Integer> fieldsToPopulate){
		if(fieldsToPopulate==null)
			System.exit(-1);
		
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
				String line=in.nextLine();
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
					if(!fieldsToPopulate.contains(i))	//the major change 
						continue;
					for(String t: tokenizer)
						fields[i]=fields[i].replaceAll(t, " ").trim();
					String[] tmp=fields[i].split(" ");
					for(String t: tmp)
						tokens.add(t.toLowerCase());
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
				String line=in.nextLine();
				String[] fields=(new CSVParser()).parseLine(line);
				
				//length check
				
				if(fields.length!=6){
					System.out.println("Error in BlockPurging! Field length != 6");
					System.out.println(line);
				}
				
				//add ID
				IDs2.add(fields[0]);
				//System.out.println(fields[0]);
				
				//build set
				Set<String> tokens=new HashSet<String>();
				for(int i=1; i<fields.length; i++){
					if(!fieldsToPopulate.contains(i))
						continue;
					for(String t: tokenizer)
						fields[i]=fields[i].replaceAll(t, " ").trim();
					String[] tmp=fields[i].split(" ");
					for(String t: tmp)
						tokens.add(t.toLowerCase());
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
	
	/*
	 * This function is designed to be deterministic. ID1s in the bilateral blocks
	 * are written out in order, as are all ID2s.Each line has the format
	 * ID1[tab]ID2_a[tab]ID2_b...[tab]ID2_c
	 * Here, we use a, b... to indicate the list of ID2s corresponding to an ID1.
	 */
	public void writeBilateralBlocksToFile(Set<String> bkvs, String outfile){
		Map<String, Set<String>> bilateralBlocks=buildBilateralBlocks(bkvs);
		ArrayList<String> keys=new ArrayList<String>(bilateralBlocks.keySet());
		Collections.sort(keys);
		PrintWriter out=null;
		try{
			out=new PrintWriter(new File(outfile));
			for(String id1:keys){
				out.print(id1+"\t");
				ArrayList<String> ids2=new ArrayList<String>(bilateralBlocks.get(id1));
				Collections.sort(ids2);
				for(int i=0; i<ids2.size()-1; i++)
					out.print(ids2.get(i)+"\t");
				out.println(ids2.get(ids2.size()-1));
			}
			
		}catch(IOException e){
			e.printStackTrace();
		}finally{out.close();}
	}
	
	public Map<String, Set<String>> buildBilateralBlocks(Set<String> bkvs){
		//blocks1 contains the IDs, with the key being a bkv.
		Map<String, Set<String>> blocks1=new HashMap<String, Set<String>>();
		//the key is an ID1, while the set contains IDs2
		Map<String, Set<String>> bilateralBlocks=new HashMap<String, Set<String>>();
		
		//populate blocks1
		for(int i=0; i<instances1.size(); i++)
			for(String bkv: bkvs)
				if(instances1.get(i).contains(bkv)){
					if(!blocks1.containsKey(bkv))
						blocks1.put(bkv, new HashSet<String>());
					blocks1.get(bkv).add(IDs1.get(i));
				}
		
		//populate bilateralBlocks
		for(int i=0; i<instances2.size(); i++)
			for(String bkv: bkvs)
				if(instances2.get(i).contains(bkv) && blocks1.containsKey(bkv))
					for(String id1: blocks1.get(bkv)){
						if(!bilateralBlocks.containsKey(id1))
							bilateralBlocks.put(id1, new HashSet<String>());
						bilateralBlocks.get(id1).add(IDs2.get(i));
					}
		return bilateralBlocks;
	}
	
	/*
	 * Computes the reduction ratio given a set of bkvs. We strictly assume
	 * token-based bkvs, whereby the bkv is contained in the sets in instances{1,2}
	 * We take pains to ensure the candidate set is deduplicated before computing RR.
	 */
	public void printReductionRatio(Set<String> bkvs){
		int exhaustiveSetSize=instances1.size()*instances2.size();
		double candidateSetSize=0.0;
		
		Map<String, Set<String>> bilateralBlocks=buildBilateralBlocks(bkvs);
		
		//update candidateSetSize 
		for(String id1: bilateralBlocks.keySet())
			candidateSetSize+=bilateralBlocks.get(id1).size();
		
		double RR=1.0-candidateSetSize/exhaustiveSetSize;
		System.out.println("Exhaustive set size is : "+exhaustiveSetSize);
		System.out.println("Dedup. candidate set size is : "+candidateSetSize);
		System.out.println("Reduction Ratio is : "+RR);
	}
	
	/*
	 *Prints pair completeness by first using the bkvs to build bilateral blocks
	 *and then compare against goldStandard 
	 * 
	 * The goldStandardFile must have a very specific format to work. It must be
	 * a CSV with a header and exactly three columns. The first column contains IDs1,
	 * the second column contains IDs2 and the third column contains a yes or no (case
	 * insensitive). If yes, we assume it is a match. 
	 * 
	 * Note that the goldStandardFile we were working with in this competition
	 * is non-exhaustive.
	 */
	public void printPairsCompleteness(Set<String> bkvs, String goldStandardFile){
		Map<String, Set<String>> goldSet=buildGoldSet(goldStandardFile, true);
		int goldPairs=0;
		for(String id1: goldSet.keySet())
			goldPairs+=goldSet.get(id1).size();
		Map<String, Set<String>> bilateralBlocks=buildBilateralBlocks(bkvs);
		int truePositives=0;
		for(String id1: goldSet.keySet())
			if(bilateralBlocks.containsKey(id1))
				for(String id2: goldSet.get(id1))
					if(bilateralBlocks.get(id1).contains(id2))
						truePositives++;
		double PC=(1.0*truePositives)/goldPairs;
		System.out.println("Number of gold true positives : "+goldPairs);
		System.out.println("Number of retrieved true positives : "+truePositives);
		System.out.println("Pairs Completeness : "+PC);
		
	}
	
	public ArrayList<String> getIDs1(){
		return IDs1;
	}
	
	public ArrayList<String> getIDs2(){
		return IDs2;
	}
	
	public Set<String> getInstances1(int index){
		return instances1.get(index);
	}
	
	public Set<String> getInstances2(int index){
		return instances2.get(index);
	}

	
	
}
