package util.bloom.Exist;

import java.math.BigInteger;
import java.nio.ByteBuffer;
/*
 * Copyright 2012 wuyou (raistlic@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.util.Arrays;

/**
 * This class is intended to provide convenient facilities for 
 * frequently used maths calculations that {@code java.lang.Math} 
 * does not cover.
 * 
 * @date   08/08/2012
 */
public class Combination {


	int num = 1024;
	static BigInteger[] FACT_RESULT_POOL = null;
	/**
	 * constructor
	 * @param _num
	 */
	  private Combination(int _num) {
		  
		  this.num=_num;
		  FACT_RESULT_POOL = new BigInteger[_num];
	  }
	  
	
	  public static BigInteger factorial(int number) {
	    
	    if( number < 0 )
	      throw new IllegalArgumentException();

	    BigInteger result = null;

	    if( number < FACT_RESULT_POOL.length )
	      result = FACT_RESULT_POOL[number];

	    if( result == null ) {

	      result = BigInteger.ONE;
	      for(int i = 2; i <= number; i++)
	        result = result.multiply(BigInteger.valueOf(i));
	      if( number < FACT_RESULT_POOL.length )
	        FACT_RESULT_POOL[number] = result;
	    }
	    return result;
	  }
	  
	  /* arr[]  ---> Input Array
	    data[] ---> Temporary array to store current combination
	    start & end ---> Staring and Ending indexes in arr[]
	    index  ---> Current index in data[]
	    r ---> Size of a combination to be printed */
	   public static void combinationUtil(int arr[], int n, int r, int index,
	                                int data[], int i)
	    {
	        // Current combination is ready to be printed, print it
	        if (index == r)
	        {
	            for (int j=0; j<r; j++)
	                System.out.print(data[j]+" ");
	            //System.out.println("");
	        return;
	        }
	 
	        // When no more elements are there to put in data[]
	        if (i >= n)
	        return;
	 
	        // current is included, put next at next location
	        data[index] = arr[i];
	        combinationUtil(arr, n, r, index+1, data, i+1);
	 
	        // current is excluded, replace it with next (Note that
	        // i+1 is passed, but index is not changed)
	        combinationUtil(arr, n, r, index, data, i+1);
	    }
	 
	   
	   public static boolean combinationXORUtil(long arr[], int n, int r, int index,
               long data[], int i,long keySum)
{
// Current combination is ready to be printed, print it
if (index == r)
{
	long value=0;
for (int j=0; j<r; j++){
	value=value^data[j];
}
if(value==keySum){
	//System.out.println("hit!: "+Arrays.toString(data)+", "+keySum+", "+value);
	return true;
}else{
	return false;
}

}

// When no more elements are there to put in data[]
if (i >= n){
	return false;
}else{
// current is included, put next at next location
data[index] = arr[i];
return combinationXORUtil(arr, n, r, index+1, data, i+1,keySum) || combinationXORUtil(arr, n, r, index, data, i+1,keySum);

// current is excluded, replace it with next (Note that
// i+1 is passed, but index is not changed)
}

}
	
	    public static boolean  printXORCombination(long arr[], int n, int r,long keySum,long data[])
	    {
	        // A temporary array to store all combination one by one
	       //long data[]=new long[r];
	 
	        // Print all combination using temprary array 'data[]'
	        return combinationXORUtil(arr, n, r, 0, data, 0,keySum);
	    }
	   
	    // The main function that prints all combinations of size r
	    // in arr[] of size n. This function mainly uses combinationUtil()
	    public static void printCombination(int arr[], int n, int r)
	    {
	        // A temporary array to store all combination one by one
	        int data[]=new int[r];
	 
	        // Print all combination using temprary array 'data[]'
	        combinationUtil(arr, n, r, 0, data, 0);
	    }
	 
	    public static long getMD5HashBase64(long key){
	    	return FineComb.sHash64.hash(ByteBuffer.allocate(8).putLong(key).array());
	    }
	    /*Driver function to check for above function*/
	    public static void main (String[] args) {
	        long arr[] = {getMD5HashBase64(1), getMD5HashBase64(2), getMD5HashBase64(3), getMD5HashBase64(4), getMD5HashBase64(5)};
	        //int r = 2;
	        int n = arr.length;
	        //printCombination(arr, n, r);
	        long v1=getMD5HashBase64(5)^getMD5HashBase64(2)^getMD5HashBase64(1);
	        
	        for(int r=1;r<=n;r++){
	        	
	        	long data[]=new long[r];
	        	if(printXORCombination(arr, n, r,v1,data)){
	        		//System.out.println("final: "+Arrays.toString(data));
	        	}
	        }
	    }

}
