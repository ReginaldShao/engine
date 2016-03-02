package com.zxbts.service.spm.engine;

import java.util.Calendar;

import com.zxbts.engine.cherry.CherryParser;
import com.zxbts.engine.cherry.VRecord;

public class FormulaEntity {
	private int cycle = 0;
	private String name;
	private Calendar firstBatch;
	private CherryParser parser;
	private VRecord record;
	private boolean valid;
	public FormulaEntity(){
		this.valid = false;
	}
	public FormulaEntity(String name,int cycle,CherryParser parser,Calendar firstBatch) {
		this.name = name;
		this.cycle = cycle;
		this.parser = parser;
		this.valid = false;
		this.firstBatch = firstBatch;
	}

	public int getCycle() {
		return cycle;
	}
	public void setCycle(int cycle) {
		this.cycle = cycle;
	}
	public CherryParser getParser() {
		return parser;
	}
	public void setParser(CherryParser parser) {
		this.parser = parser;
	}
	public VRecord getRecord() {
		return record;
	}
	public void setRecord(VRecord record) {
		this.record = record;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Calendar getFirstBatch() {
		return firstBatch;
	}
	public void setFirstBatch(Calendar firstBatch) {
		this.firstBatch = firstBatch;
	}
	public double calculate(){
		return this.parser.calculate(this.record);
	}
	public boolean isValid() {
		return valid;
	}
	public void setValid(boolean valid) {
		this.valid = valid;
	}
	
}
