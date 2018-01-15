package com.wzy.tq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Weather implements WritableComparable<Weather>{

	private int year;
	private int month;
	private int day;
	private int temperature;
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getTemperature() {
		return temperature;
	}

	public void setTemperature(int temperature) {
		this.temperature = temperature;
	}

	
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(temperature);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.month = in.readInt();
		this.day = in.readInt();
		this.temperature = in.readInt();
	}

	@Override
	public int compareTo(Weather o) {
		int c1 = Integer.compare(this.year, o.getYear());
		if (c1 == 0) {
			int c2 = Integer.compare(this.month, o.getMonth());
			if (c2 == 0) {
				return Integer.compare(this.temperature, o.getTemperature());
			}
			return c2;
		}
		return c1;
	}

}









