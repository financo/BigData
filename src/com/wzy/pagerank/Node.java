package com.wzy.pagerank;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

public class Node {
	
	private double pageRank = 1.0;
	
	private String[] adjacentNodeNames;
	
	public static final char fieldSeperator = '\t';

	public double getPageRank() {
		return pageRank;
	}

	public Node setPageRank(double pageRank) {
		this.pageRank = pageRank;
		return this;
	}

	public String[] getAdjacentNodeNames() {
		return adjacentNodeNames;
	}

	public Node setAdjacentNodeNames(String[] adjacentNodeNames) {
		this.adjacentNodeNames = adjacentNodeNames;
		return this;
	}
	
	public boolean containsAdjacentNodes() {
		return adjacentNodeNames != null && adjacentNodeNames.length > 0;
	}

	public static char getFieldseperator() {
		return fieldSeperator;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(pageRank);
		if (getAdjacentNodeNames() != null) {
			sb.append(fieldSeperator).append(StringUtils.join(getAdjacentNodeNames(), fieldSeperator));
		}
		return sb.toString();
	}
	
	public static Node fromMR(String value) throws IOException {
		String[] parts = StringUtils.splitPreserveAllTokens(value, fieldSeperator);
		if (parts.length < 1) {
			throw new IOException("Exception 1 or more parts but received " + parts.length);
		}
		
		Node node = new Node().setPageRank(Double.valueOf(parts[0]));
		if (parts.length > 1) {
			node.setAdjacentNodeNames(Arrays.copyOfRange(parts, 1, parts.length));
		}
		return node;
	}
	
}
