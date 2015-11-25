package se.ugli.bigqueue;

class Calculator {

    public static long mod(final long val, final int bits) {
        return val - (val >> bits << bits);
    }

    public static long mul(final long val, final int bits) {
        return val << bits;
    }

    public static long div(final long val, final int bits) {
        return val >> bits;
    }

}
