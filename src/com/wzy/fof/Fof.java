package com.wzy.fof;

public class Fof {
	public String format(String friend1, String friend2) {
		int c = friend1.compareTo(friend2);
		if (c < 0) {
			return friend2 + "-" + friend1;
		}
		return friend1 + "-" + friend2;
	}
}
