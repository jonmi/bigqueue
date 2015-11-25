package se.ugli.bigqueue.utils;

public class Calculator {

    public static long mod(long val, int bits) {
        return val - ((val >> bits) << bits);
    }

    public static long mul(long val, int bits) {
        return val << bits;
    }

    public static long div(long val, int bits) {
        return val >> bits;
    }

}
