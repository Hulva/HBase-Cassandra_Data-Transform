package hulva.luva.wxx.tools.datatransform.hbasecassandra.utils;

import java.util.Random;

public class RandomUtils {
	
	private static final String BaseChars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
	private static final Random random = new Random();
	
	public static String nextString(int length){
		if(length <= 0){
			return "";
		}
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < length; i++) {
			 int number = random.nextInt(BaseChars.length());   
		     sb.append(BaseChars.charAt(number));  
		}
		return sb.toString();
	}
	
}
