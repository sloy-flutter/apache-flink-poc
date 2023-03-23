package com.flutter.pojos;

public class Bet {
    public String runnerName;
    public Integer liability;

    public Bet(String runnerName, Integer liability) {
        this.runnerName = runnerName;
        this.liability = liability;
    }

    @Override
    public String toString() {
        return "{" +
                "\"runnerName\":\"" + runnerName + "\", " +
                "\"liability\":" + liability +
                '}';
    }
}
