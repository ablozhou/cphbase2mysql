package my.test;

import java.io.UnsupportedEncodingException;

public class Test {

	
	public static void printHexString( byte[] b) { 
		for (int i = 0; i < b.length; i++) {
			String hex = Integer.toHexString(b[i] & 0xFF);
			if (hex.length() == 1) {
				hex = '0' + hex;
			}
			System.out.print("\\x" + hex.toUpperCase());

		}
		System.out.println();
	}
	public static void main(String[] args) throws UnsupportedEncodingException {
		
		String str="周海汉";
		byte[] bs = str.getBytes();
		printHexString(bs);
		
		String str1 = new String(bs,"GBK");
		byte[] bs1 = str1.getBytes();
		printHexString(bs1);
		String str2 = new String(bs1,"UTF-16");
		byte[] bs2 = str2.getBytes();
		printHexString(bs2);
		System.out.println(str);
		
		
		
	}

}
