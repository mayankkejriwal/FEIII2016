package matching;

import java.util.*;
import blocking.BlockPurging;

public class Exploratory {
	/**
	 * My main goal in writing this class is to try out various things on the
	 * ground truth. Based on that, I will implement the final matcher in
	 * a separate class. Note that for computing Jaccard, I am going
	 * to be using the instance information in the files, not the gold standard
	 * file. We will try with all fields, then with certain subsets of fields etc.
	 * All data is recorded in the JaccardGoldStandardAnalysis excel file.
	 * 
	 * We will also re-use computeJaccard in other classes.
	 */
	
	public static void printJaccardScoresGoldStandard(String file1, String file2, 
			String goldStandardFile){
		
		Set<Integer> fieldsToPopulate=new HashSet<Integer>();
		int[] fields={1,2};
		for(int field:fields)
			fieldsToPopulate.add(field);
		
		BlockPurging setup=new BlockPurging(file1, file2, fieldsToPopulate);
		Map<String, Integer> IDs1Map=BlockPurging.buildIDMap(setup.getIDs1());
		Map<String, Integer> IDs2Map=BlockPurging.buildIDMap(setup.getIDs2());
		Map<String, Set<String>> posGoldSet=BlockPurging.buildGoldSet(goldStandardFile, true);
		Map<String, Set<String>> negGoldSet=BlockPurging.buildGoldSet(goldStandardFile, false);
		
		
		System.out.println("Printing Jaccard Scores for Positive Set...");
		System.out.println("ID1\tID2\tJaccardScore");
		for(String id1: posGoldSet.keySet())
			for(String id2: posGoldSet.get(id1)){
				Set<String> instance1=setup.getInstances1(IDs1Map.get(id1));
				Set<String> instance2=setup.getInstances2(IDs2Map.get(id2));
				System.out.println(id1+"\t"+id2+"\t"+computeJaccard(instance1, instance2));
			}
		
		
		
		System.out.println("Printing Jaccard Scores for Negative Set...");
		System.out.println("ID1\tID2\tJaccardScore");
		for(String id1: negGoldSet.keySet())
			for(String id2: negGoldSet.get(id1)){
				Set<String> instance1=setup.getInstances1(IDs1Map.get(id1));
				Set<String> instance2=setup.getInstances2(IDs2Map.get(id2));
				System.out.println(id1+"\t"+id2+"\t"+computeJaccard(instance1, instance2));
			}
		
	}
	
	private static <T>int unionCardinality(Set<T> set1, Set<T> set2){
		int result=set1.size();
		for(T t: set2)
			if(!set1.contains(t))
				result++;
		return result;
	}
	
	private static <T>int intersectionCardinality(Set<T> set1, Set<T> set2){
		int result=0;
		for(T t: set1)
			if(set2.contains(t))
				result++;
		return result;
	}
	
	public static double computeJaccard(Set<String> set1, Set<String> set2){
		
		
		int union=unionCardinality(set1, set2);
		if(union==0){
		//	System.out.println("Problems: union is 0");
			return -1.0;
		}
		int intersection=intersectionCardinality(set1, set2);
		
		return 1.0*intersection/union;
	}

}
