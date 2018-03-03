package com.refactorlabs.cs378.assign2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class WordStatisticsWritable implements WritableComparable<LongWritable> {
	
	private long paragraph_count; // Number of paragraphs this word occurs in
	private double word_count; // Total number of times word is in document
	private double squared; // word_count squared, used for variance

	public WordStatisticsWritable() {
		this.set(0, 0, 0);
	}
	
	public WordStatisticsWritable(long paragraph, double occurances, double sq) {
		this.set(paragraph, occurances, sq);
	}
	
	/* 
	 * Sets the private variables to the desired values.
	 */
	private void set(long paragraph, double occurances, double sq) {
		this.paragraph_count = paragraph;
		this.word_count = occurances;
		this.squared = sq;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.paragraph_count = in.readLong();
		this.word_count = in.readDouble();
		this.squared = in.readDouble();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.paragraph_count);
		out.writeDouble(this.word_count);
		out.writeDouble(this.squared);
	}

	@Override
	public int compareTo(LongWritable o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	/* 
	 * Returns the number of paragraphs this word appears in 
	 */
	public long getParagraphCount() {
		return this.paragraph_count;
	}
	
	/* 
	 * Returns the number of times this word appears in the document 
	 */
	public double getWordCount() {
		return this.word_count;
	}
	
	/* 
	 * Returns the number of times this word appears in the document squared.
	 * Used for calculating variance.
	 */
	public double getWordCountSquared() {
		return this.squared;
	}
	
	public String toString() {
		return this.paragraph_count + "," + this.word_count + "," + this.squared;
	}
	
	public boolean equals(Object other) {
		if(this.paragraph_count == ((WordStatisticsWritable) other).paragraph_count &&
		   this.word_count == ((WordStatisticsWritable) other).word_count &&
		   this.squared == ((WordStatisticsWritable)  other).squared) {
			return true;
		}
		else {
			return false;
		}
	}

}
