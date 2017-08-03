package com.packt.sfjd.ch9;

import java.util.Scanner;

public class Calculator {

	private static final String EXIT = "EXIT";

	public static void main(String[] args) {

		Calculator calc = new Calculator();
		Scanner s = new Scanner(System.in);
		while (true) {
			String res = calc.runCalc(s);
			if (res.equals(EXIT)) {
				break;
			} else {
				System.out.println(res);
			}
		}
	}

	private String runCalc(Scanner s) {
		System.out.println("Main Menu:");
		System.out.println("1. Addition");
		System.out.println("2. Substraction");
		System.out.println("3. Multipication");
		System.out.println("4. Division");
		System.out.println("5. Exit");
		System.out.println("Enter your choice: ");
		int i = s.nextInt();

		if (i == 5) {
			return EXIT;
		}

		System.out.println("ENTER FIRST NUMBER ");
		int a = s.nextInt();

		System.out.println("ENTER SECOND NUMBER ");
		int b = s.nextInt();

		int result = 0;// 'result' will store the result of operation

		switch (i) {
		case 1:
			result = a + b;
			break;
		case 2:
			result = a - b;
			break;
		case 3:
			result = a * b;
			break;
		case 4:
			result = a / b;
			break;

		default:
			return "Wrong Choice.";

		}

		return "Answer is " + result;
	}
}
