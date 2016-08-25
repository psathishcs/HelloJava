package org.hello;

import static org.junit.Assert.*;

import org.junit.Test;

public class HelloTestCase {
	
	String message ="Tamil Nadu";
	@Test
	public void testPrintMessage() {
		System.out.println("Inside testPringMessage!");
		assertEquals(message, "Tamil Nadu");
	}

}
