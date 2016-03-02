package com.zxbts.service.spm.engine;

import java.util.HashMap;
import java.util.Map;

import com.zxbts.engine.cherry.VField;
import com.zxbts.engine.cherry.VTable;

public class Table implements VTable {
	private String tableName ;
	private Map<String, VField> hashMap = new HashMap<String, VField>();

	public Table(String parentId){
		this.tableName = parentId;
	}
	@Override
	public Map<String, VField> getSchema() {
		return this.hashMap;
		//String:表名; VField:字段信息
		//根据ID获得target的所有依赖项
		//先获得所有子目标列表，然后以此判断子目标是否有数据
		//接着读取所有子指标，判断是否有数据
	}

	@Override
	public String getName() {
		//表名
		return tableName;
	}
	
	public void addField(String key, VField value) {
		this.hashMap.put(key, value);
	}
}
