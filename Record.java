package com.zxbts.service.spm.engine;

import java.util.HashMap;
import java.util.Map;

import com.zxbts.engine.cherry.VField;
import com.zxbts.engine.cherry.VRecord;

public class Record implements VRecord {
	private Map<String,VField> field = new HashMap<String,VField>();
	//存储的是ID和值
	@Override
	public VField getField(String name) {
		return this.field.get(name);
	}
	
	public void addField(String name,VField value){
		this.field.put(name, value);
	}

}
