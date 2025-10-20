package org.howard.edu.lsp.midterm.question2;


public class Main {
    public static void main(String[] args) {
        System.out.println("Circle radius 3.0 → area = " + AreaCalculator.area(3.0));
        System.out.println("Rectangle 5.0 x 2.0 → area = " + AreaCalculator.area(5.0, 2.0));
        System.out.println("Triangle base 10, height 6 → area = " + AreaCalculator.area(10, 6));
        System.out.println("Square side 4 → area = " + AreaCalculator.area(4));
        try {
        	System.out.println("Circle radius -8 → area = " + AreaCalculator.area(-8));
        } catch (IllegalArgumentException e) {
        	System.out.println("Caught exception: " + e.getMessage());
        }
        
    }
}