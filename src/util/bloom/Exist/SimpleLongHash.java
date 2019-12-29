package util.bloom.Exist;

import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import edu.harvard.syrah.prp.POut;
import util.bloom.Apache.Key;
import util.bloom.Apache.Hash.MurmurHash3;
import util.bloom.Apache.Hash.MurmurHash3.LongPair;

public class SimpleLongHash {
    MessageDigest md;
    //
    
    public SimpleLongHash(){
        try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    //
    public long hash(final String str) {
        return hash(str.getBytes());
    }
    public long hash0(final byte[] buf) {
        md.reset();
        final byte[] digest = md.digest(buf);
        return (getLong(digest, 0) ^ getLong(digest, 8));
    }
       
    public long hash(Key key) {
    	byte[] buf = key.getBytes();
        md.reset();
        final byte[] digest = md.digest(buf);
        return (getLong(digest, 0) ^ getLong(digest, 8));
    } 
    
    /**
     * get the 256 byte hash
     * @param buf
     * @return
     */
    public String hash2String(final byte[] buf){
    	md.reset();
    	md.update(buf, 0, buf.length);
    	byte[] vector = md.digest();
    	StringBuffer sb = new StringBuffer();    	
    	if (vector != null) {
    	  for (int i = 0; i < vector.length; i++) {
    		sb.append(Integer.toHexString(vector[i]&0xff));    		
    	}
    	}
    	return sb.toString();
    }
    /**
     * get hash
     * @param buf
     * @return
     */
    public long hash(final byte[] buf){
		int offset=0;
		//byte[] bytes = key.getBytes();
		int seed=0x7a43a1e9;
		int m2 = 0x2619cad4;
		LongPair out = new LongPair();
		MurmurHash3.murmurhash3_x64_128(buf, offset, buf.length, seed, out);
		long hashCode = out.val1+m2*out.val1;
		return hashCode;
    }
    
    //
    private static final long getLong(final byte[] array, final int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = ((value << 8) | (array[offset+i] & 0xFF));
        }
        return value;
    }
}
