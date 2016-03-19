package matching;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import blocking.BlockPurging;

public class JaccardMatcher {
/**
 * I will use this class for implementing our Jaccard matcher. At present, I will assume
 * the blocks on the fly, and match on the fly, then write out to file accordingly.
 *  
 */
	/*
	 * This function is hard-coded. nonStrictOutFile contains those pairs of records
	 * that have Jaccard score >=0.5, while for strictOutFile it is >0.5. We will
	 * generate both simultaneously. The files are generated deterministically.
	 * 
	 * Note that these are not the final submission finals as they
	 * (1) don't contain header
	 * (2) contain three columns, including score. 
	 * 
	 * Thus, the files will have to be appropriately formatted before submission.
	 * Note that all delimiters are , not [tab].
	 */
	public static void hardCodedPointFive(String instanceFile1, String instanceFile2,
			String nonStrictOutFile, String strictOutFile){
		long start=System.currentTimeMillis();
		BlockPurging obj=new BlockPurging(instanceFile1, instanceFile2);
		PrintWriter nonStrictOut=null;
		PrintWriter strictOut=null;
		Map<String, Set<String>> bilateralBlocks=obj.buildBilateralBlocks(obj.buildValidCommonKeySet());
		ArrayList<String> id1s=new ArrayList<String>(bilateralBlocks.keySet());
		Map<String, Integer> id1Map=BlockPurging.buildIDMap(obj.getIDs1());
		Map<String, Integer> id2Map=BlockPurging.buildIDMap(obj.getIDs2());
		
		
		Collections.sort(id1s);
		try{
			nonStrictOut=new PrintWriter(new File(nonStrictOutFile));
			strictOut=new PrintWriter(new File(strictOutFile));
			
			
			for(String id1: id1s){
				Set<String> instance1=obj.getInstances1(id1Map.get(id1));
				ArrayList<String> ids2=new ArrayList<String>(bilateralBlocks.get(id1));
				Collections.sort(ids2);
				for(String id2: ids2){
					Set<String> instance2=obj.getInstances2(id2Map.get(id2));
					double jaccardScore=Exploratory.computeJaccard(instance1, instance2);
					if(jaccardScore<0.5)
						continue;
					else if(jaccardScore==0.5){
						nonStrictOut.println(id1+","+id2+","+jaccardScore);
					}else{
						nonStrictOut.println(id1+","+id2+","+jaccardScore);
						strictOut.println(id1+","+id2+","+jaccardScore);
					}
				}
			}
			
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			nonStrictOut.close();
			strictOut.close();
		}
		
		long end=System.currentTimeMillis();
		double minutesTaken=(1.0*(end-start))/(60000);
		System.out.println("Matcher complete. Time taken "+minutesTaken+" minutes");
		
	}


	/*
	 * This function outputs the three submission files for the FFIEC-SEC task, with 
	 * Jaccard scores >0.4, >=0.5, and >0.5 respectively. All files
	 * are generated deterministically and simultaneously. The three files above
	 * are named per the following guidelines:
	 * UT_AUSTIN_FFIEC_SEC_TP_{1,2,3} for the three files above.
	 * 
	 */
	public static void submissionFFIECSEC(String instanceFile1, String instanceFile2,
			String outputFolder){
		long start=System.currentTimeMillis();
		int field=1;
		Set<Integer> fieldsToPopulate=new HashSet<Integer>();
		fieldsToPopulate.add(field);
		BlockPurging obj=new BlockPurging(instanceFile1, instanceFile2, fieldsToPopulate);
		PrintWriter file1=null;
		PrintWriter file2=null;
		PrintWriter file3=null;
		
		Map<String, Set<String>> bilateralBlocks=obj.buildBilateralBlocks(obj.buildValidCommonKeySet());
		ArrayList<String> id1s=new ArrayList<String>(bilateralBlocks.keySet());
		Map<String, Integer> id1Map=BlockPurging.buildIDMap(obj.getIDs1());
		Map<String, Integer> id2Map=BlockPurging.buildIDMap(obj.getIDs2());
		
		
		Collections.sort(id1s);
		try{
			file1=new PrintWriter(new File(outputFolder+"UT_AUSTIN_FFIEC_SEC_TP_1"));
			file2=new PrintWriter(new File(outputFolder+"UT_AUSTIN_FFIEC_SEC_TP_2"));
			file3=new PrintWriter(new File(outputFolder+"UT_AUSTIN_FFIEC_SEC_TP_3"));
			String header="FFIEC_IDRSSD,SEC_CIK";
			file1.println(header);
			file2.println(header);
			file3.println(header);
			
			for(String id1: id1s){
				Set<String> instance1=obj.getInstances1(id1Map.get(id1));
				ArrayList<String> ids2=new ArrayList<String>(bilateralBlocks.get(id1));
				Collections.sort(ids2);
				for(String id2: ids2){
					Set<String> instance2=obj.getInstances2(id2Map.get(id2));
					double jaccardScore=Exploratory.computeJaccard(instance1, instance2);
					if(jaccardScore<=0.4)
						continue;
					else if(jaccardScore<0.5){
						file1.println(id1+","+id2);
					}
					else if(jaccardScore==0.5){
						file1.println(id1+","+id2);
						file2.println(id1+","+id2);
					}else{
						file1.println(id1+","+id2);
						file2.println(id1+","+id2);
						file3.println(id1+","+id2);
					}
				}
			}
			
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			file1.close();
			file2.close();
			file3.close();
		}
		
		long end=System.currentTimeMillis();
		double minutesTaken=(1.0*(end-start))/(60000);
		System.out.println("Matcher complete. Time taken "+minutesTaken+" minutes");
		
	}


	/*
	 * This function outputs the three submission files for the FFIEC-SECLEI task, with 
	 * Jaccard scores >0.4, >=0.5, and >0.5 respectively. All files
	 * are generated deterministically and simultaneously. The three files above
	 * are named per the following guidelines:
	 * UT_AUSTIN_FFIEC_LEI_TP_{1,2,3} for the three files above.
	 * 
	 */
	public static void submissionFFIECLEI(String instanceFile1, String instanceFile2,
			String outputFolder){
		long start=System.currentTimeMillis();
		int field=1;
		Set<Integer> fieldsToPopulate=new HashSet<Integer>();
		fieldsToPopulate.add(field);
		BlockPurging obj=new BlockPurging(instanceFile1, instanceFile2, fieldsToPopulate);
		PrintWriter file1=null;
		PrintWriter file2=null;
		PrintWriter file3=null;
		
		Map<String, Set<String>> bilateralBlocks=obj.buildBilateralBlocks(obj.buildValidCommonKeySet());
		ArrayList<String> id1s=new ArrayList<String>(bilateralBlocks.keySet());
		Map<String, Integer> id1Map=BlockPurging.buildIDMap(obj.getIDs1());
		Map<String, Integer> id2Map=BlockPurging.buildIDMap(obj.getIDs2());
		
		
		Collections.sort(id1s);
		try{
			file1=new PrintWriter(new File(outputFolder+"UT_AUSTIN_FFIEC_LEI_TP_1"));
			file2=new PrintWriter(new File(outputFolder+"UT_AUSTIN_FFIEC_LEI_TP_2"));
			file3=new PrintWriter(new File(outputFolder+"UT_AUSTIN_FFIEC_LEI_TP_3"));
			String header="FFIEC_IDRSSD,LEI_LEI";
			file1.println(header);
			file2.println(header);
			file3.println(header);
			
			for(String id1: id1s){
				Set<String> instance1=obj.getInstances1(id1Map.get(id1));
				ArrayList<String> ids2=new ArrayList<String>(bilateralBlocks.get(id1));
				Collections.sort(ids2);
				for(String id2: ids2){
					Set<String> instance2=obj.getInstances2(id2Map.get(id2));
					double jaccardScore=Exploratory.computeJaccard(instance1, instance2);
					if(jaccardScore<=0.4)
						continue;
					else if(jaccardScore<0.5){
						file1.println(id1+","+id2);
					}
					else if(jaccardScore==0.5){
						file1.println(id1+","+id2);
						file2.println(id1+","+id2);
					}else{
						file1.println(id1+","+id2);
						file2.println(id1+","+id2);
						file3.println(id1+","+id2);
					}
				}
			}
			
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			file1.close();
			file2.close();
			file3.close();
		}
		
		long end=System.currentTimeMillis();
		double minutesTaken=(1.0*(end-start))/(60000);
		System.out.println("Matcher complete. Time taken "+minutesTaken+" minutes");
		
	}
}
