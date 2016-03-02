package com.zxbts.service.spm.engine;

import com.zxbts.engine.cherry.VField;

public class Field implements VField {
	private String name;
	private double value;
	private TYPE type;
	
	public Field(String id,TYPE type){
		this.name = id;
		this.type = type;
	}
	public Field(String id ,TYPE type,double value){
		this.name = id;
		this.type = type;
		this.value = value;
	}
	@Override
	public String getName() {
		return name;
	}

	@Override
	public TYPE getType() {
		return type;
	}

	@Override
	public double getValue() {
		return this.value;
	}
	public void setValue(double value) {
		this.value = value;
	}
	
}
