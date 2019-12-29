package util.async;

public class UniformHashFunc {

	
	public static int  os_dietz_thorup32(int x, int bins, long a, long b){
		  // ((ax mod 2**62) + b) mod 2 ** 62, hi 32, pad 0 on left
		  // = (ax + b) mod 2 ** 62, hi 32, pad 0 on the left

		  return ((int) ((a*x+b) >> 32)) % bins;
		  // mod 64 then top 32 bits 
		  // (this impl. okay for little endian)
		  // wiki says just ax .. is 2-universal
		  // why is it strongly universal/ 2-wise indep.?
		}

	// 2-universal hashing, assumes bins power of 2, very fast
	short os_dietz8to3(short x, short a) {
	  return (short)(((a*x)  >> 5) & 7);
	}
	
	 public static long os_dietz8to3(long x, long a) {
		  return (long)(((a*x)  >> 5) & 7);
		}
	
	
}
