package com.zxbts.service.spm.engine;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.zxbts.engine.cherry.CherryEngine;
import com.zxbts.engine.cherry.CherryParser;
import com.zxbts.engine.cherry.VField;
import com.zxbts.engine.cherry.VField.TYPE;
import com.zxbts.frame.ServiceContext;
import com.zxbts.service.spm.engine.BatchMapping.MappingSpace;
import com.zxbts.service.spm.model.*;

public class Engine {
	public static int MAX_CYCLE_NUIT = 3;
	public static int MIN_CYCLE_UNIT = 1;
	private QueryDao dao = null;
	private Target target;
	private BatchMapping batchMapping;
	private static Map<String, FormulaEntity> formulaMap = new ConcurrentHashMap<String, FormulaEntity>();
	private static Map<String,Calendar> batchMap = new ConcurrentHashMap<String,Calendar>();
	private DataUnit dataUnit;
	
	public Engine(DataUnit dataUnit) {
		this.dataUnit = dataUnit;
	}
	
	public void calcAndWrite(){
		dataCollection();
		//删除数据库中的记录
		this.dao.deleteData(this.dataUnit.getTarget(), this.dataUnit.getDate());
	}
	
	private void dataCollection(){
		String indicatorId = this.dataUnit.getTarget();
		String batchNo = ""+this.dataUnit.getDate();
		List<DataUnit> resultList = new ArrayList<DataUnit>();
		Indicator indicator;
		MappingSpace mappingSpace;
		int cycle=0;
		int cycle_unit=0;
		String targetId;
		
		try {
			this.dao = SPMDaoFactory.getInstance().getQueryDao();
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException("engine: getQueryDao: ReflectiveOperationException",e);
		}
		
		indicator = this.dao.getIndicator(indicatorId);
		if(indicator == null){
			ServiceContext.getInstance().getLogger().warn("failed to get indicator---indicatorId:"+indicatorId);
			return;
		}
		Strategy strategy = this.dao.getStrategy(indicator.getStrategyId());
		if(strategy == null){
			ServiceContext.getInstance().getLogger().warn("failed to get strategy---strategyId:"+indicator.getStrategyId());
			return;
		}
		String strategyBegin = ""+strategy.getBegin();
		targetId = indicator.getParent();
		cycle = indicator.getCycle();
		cycle_unit = indicator.getCycleUnit();
		
		this.target = this.dao.getTarget(targetId);
		if(this.target == null){
			ServiceContext.getInstance().getLogger().info("failed to get target----targetId:"+targetId);
			return;
		}
		
		if(!this.target.getFormular().contains(indicatorId)) return;
		
		//获得对父target的影响范围
		this.batchMapping = new BatchMapping();
		mappingSpace = this.batchMapping.getMappingBatch(cycle, cycle_unit, batchNo);
		
		//以target的采集周期为单位，循环mappingSpace的区间
		Calendar begin_time = BatchMapping.string2Calendar(mappingSpace.getBegin(),BatchMapping.PATTERN);
		Calendar end_time = Calendar.getInstance();
		Date next_batch = null;
		DataUnit element = null;
		
		while(this.target != null){
			//根据目标的采集周期,计算上一批次号，则下一次indicator和target的对应域的上限就是该批次号
			end_time.setTime( BatchMapping.string2Calendar(mappingSpace.getEnd(),BatchMapping.PATTERN).getTime() );
			//在目标的影响域内，进行子目标的计算。
			while(true){
				next_batch = getNextBatchNo(end_time,this.target.getCycle());
				if(next_batch.after(begin_time.getTime())){
					end_time.setTime(next_batch);
					element = targetCalc(next_batch,strategyBegin);
					if(element != null){
						resultList.add(element);
					}
				}else{
					//使用begin_time作为对应域的上限
					next_batch = begin_time.getTime();
					element = targetCalc(next_batch,strategyBegin);
					if(element != null){
						resultList.add(element);
					}
					break;
				}
				
			}
			
			if(resultList != null){
				for(DataUnit ele:resultList){
					if(this.dao.updateData(ele)){
						ServiceContext.getInstance().getLogger().info("update success!---ID: "+ele.getTarget()+"---"+ele.getDate()+"---"+ele.getValue());
					}
				}
			}
			
			this.target = this.dao.getTarget(this.target.getParentId());
		}
		return;
	}
	
	private DataUnit targetCalc(Date batchUpperBound,String strategyBegin) {
		double result = 0;
		DataUnit element = new DataUnit();
		FormulaEntity formulaEntity;
		String targetId = this.target.getId();

		element.setTarget(targetId);
		element.setDate(Integer.valueOf(new SimpleDateFormat().format(batchUpperBound)));// 设置批次号
		element.setType(1);
		element.setInx(this.target.getType());
		element.setStrategyId(this.target.getStrategyId());

		// 加入互斥控制
		synchronized (Engine.class) {
			// 判断父target是否在全局的公式计算对象里
			formulaEntity = Engine.formulaMap.get(targetId);
			if (formulaEntity == null) {
				formulaEntity = createFormulaEntity(targetId,strategyBegin);
				if (formulaEntity == null) {
					ServiceContext.getInstance().getLogger()
							.info("failed to createFormulaEntity---targetId:" + targetId);
					return null;
				}
				Engine.formulaMap.put(targetId, formulaEntity);
			}
		}
		if(batchUpperBound.before(formulaEntity.getFirstBatch().getTime())){
			ServiceContext.getInstance().getLogger().info("batchUpperBound before firstBatch of:" + targetId+",no data exist");
			return null;
		}
		// 此时，formulaEntity对应的Record不完整，需要建立。
		List<VField> fieldList = formulaEntity.getParser().getFieldList();
		Record record = createRecord(fieldList, batchUpperBound,formulaEntity);
		if (record == null) {
			ServiceContext.getInstance().getLogger().info("failed to createRecord---targetId:" + targetId);
			return null;
		}
		formulaEntity.setRecord(record);

		result = formulaEntity.calculate();
		element.setValue(result);
		return element;
	}
	
	private Date getNextBatchNo(Calendar time, int cycle) {
		switch(cycle){
		case 1:
			time.add(Calendar.DAY_OF_YEAR, -1);
			break;
		case 2:
			time.add(Calendar.MONTH, -1);
			break;
		case 3:
			time.add(Calendar.YEAR, -1);
			break;
		}
		return time.getTime();
	}
	
	private FormulaEntity createFormulaEntity(String targetId,String strategyBegin){
		
		FormulaEntity formulaEntity= null;
		Table table = new Table(targetId);
		
		if(this.target == null){
			return null;
		}
		
		int cycle = getTargetCycle(targetId);
		if(cycle == 0){
			ServiceContext.getInstance().getLogger().info("failed to calculate target cycle---targetId:"+targetId);
			return null;
		}
		this.target.setCycle(cycle);
		
		//读取target的所有子目标和子指标，创建虚拟表
		String id;
		TYPE type;
		
		List<Indicator> indicatorList = this.target.getSubIndicators();
		if(indicatorList != null){
			for(Indicator i : indicatorList){
				id = i.getId();
				if(i.getType() == 1){
					type = TYPE.INT;
				}else{
					type = TYPE.DOUBLE;
				}
				table.addField(id, new Field(id, type));
			}
		}
		
		List<Target> targetList = this.target.getSubTargets();
		if(targetList != null){
			for(Target t:targetList){
				id = t.getId();
				if(t.getType() == 1){
					type = TYPE.INT;
				}else{
					type =TYPE.DOUBLE;
				}
				table.addField(id, new Field(id,type));
			}
		}//建表完成
		
		//生成parser
		CherryParser parser = null;
		String formula = this.target.getFormular();
		if(formula != null){
			try {
				parser = CherryEngine.getInstance().parse(table, formula);
			} catch (com.zxbts.engine.cherry.javacc.ParseException e) {
				throw new RuntimeException("failed to get parser",e);
			}
			
			Calendar firstBatch = calcFirstBatchOfTarget(targetId, strategyBegin);
			Engine.batchMap.put(targetId, firstBatch);
			formulaEntity = new FormulaEntity(this.target.getId(),cycle,parser,firstBatch);
		}
		
		return formulaEntity;
	}
	
	//读取cycle
	private int getTargetCycle(String targetId){
		Target target;
		int cycle = 0;
		
		target = this.dao.getTarget(targetId);
		if(target == null){
			return cycle;
		}
		
		cycle = target.getCycle();
		if( cycle == 0){
			//通过读取子目标和子指标的采集周期，获得当前target的采集周期
			cycle = MAX_CYCLE_NUIT ;
			int tmp=0;
			
			List<Indicator> indicatorList = target.getSubIndicators();
			if(indicatorList != null){
				//判断其所有的采集周期最小值
				for(Indicator i : indicatorList){
					tmp = i.getCycleUnit();
					if( tmp < cycle ){
						cycle = tmp;
					}
					if(cycle == MIN_CYCLE_UNIT){
						this.dao.updateTargetCycle(targetId, cycle);
						return cycle;
					}
				}
			}
			
			//接下来还需要继续对子目标进行逐个排查
			List<Target> targetList = target.getSubTargets();
			if(targetList != null){
				for(Target t:targetList){
					tmp = getTargetCycle( t.getId() );
					if(tmp < cycle){
						cycle = tmp;
					}
					if(cycle == MIN_CYCLE_UNIT){
						this.dao.updateTargetCycle(targetId, cycle);
						return cycle;
					}
				}
			}
			
			if(cycle != 0){
					this.dao.updateTargetCycle(targetId, cycle);
			}
		}
		
		return cycle;
	}
	
	//计算target的第一个开始批次号
	private Calendar calcFirstBatchOfTarget(String targetId,String strategyBegin){
		if(Engine.batchMap.containsKey(targetId)){
			Calendar calendar = Calendar.getInstance();
			calendar.setTime( Engine.batchMap.get(targetId).getTime() );
			return calendar;
		}
		Target target = this.dao.getTarget(targetId);
		List<Indicator> indicatorList = target.getSubIndicators();
		List<Target> targetList = target.getSubTargets();
		
		Calendar biggest = Calendar.getInstance();
		biggest.setTimeInMillis(0);
		
		String name =null;
		for(Indicator i:indicatorList){
			Calendar tmpCalendar = Calendar.getInstance();
			name = i.getId();
			if(Engine.batchMap.containsKey(name)){
				tmpCalendar.setTime( Engine.batchMap.get(name).getTime() );
				if(biggest.before(tmpCalendar)){
					biggest.setTime(tmpCalendar.getTime());
				}
			}else{
				tmpCalendar.setTime( calcFirstBatchOfIndicator(i,strategyBegin).getTime() );
					if(!Engine.batchMap.containsKey(name)){
						Engine.batchMap.put(name, tmpCalendar);
					}
				
				if(biggest.before(tmpCalendar)){
					biggest.setTime(tmpCalendar.getTime());
				}
			}
		}
		
		for(Target t:targetList){
			Calendar tmpCalendar = Calendar.getInstance();
			name = t.getId();
			if(Engine.batchMap.containsKey(name)){
				tmpCalendar.setTime( Engine.batchMap.get(name).getTime() );
				if(biggest.before(tmpCalendar)){
					biggest.setTime(tmpCalendar.getTime());
				}
			}else{
				tmpCalendar.setTime( calcFirstBatchOfTarget(name,strategyBegin).getTime() );
					if(!Engine.batchMap.containsKey(name)){
						Engine.batchMap.put(name, tmpCalendar);
					}
				
				if(biggest.before(tmpCalendar)){
					biggest.setTime(tmpCalendar.getTime());
				}
			}
		}
		
		return biggest;
	}

	//计算indicator的第一个开始批次号
	private Calendar calcFirstBatchOfIndicator(Indicator indicator, String strategyBegin){
		Calendar beginBatch = BatchMapping.string2Calendar(strategyBegin, BatchMapping.PATTERN);
		
		String offset = indicator.getOffset();
		int[] off;
		if(offset != null){
			off = BatchMapping.parseOffset(offset);
		}else{
			off = new int[]{0,0,0};
		}
		
		switch(indicator.getCycleUnit()){
		case 1:
			beginBatch.add(Calendar.DAY_OF_YEAR,indicator.getCycle());
			beginBatch.getTime();
			beginBatch.add(Calendar.DAY_OF_YEAR,off[2]);
			beginBatch.getTime();
			break;
		case 2:
			beginBatch.add(Calendar.MONTH,indicator.getCycle());
			beginBatch.getTime();
			beginBatch.set(Calendar.DAY_OF_MONTH,1);
			beginBatch.getTime();
			beginBatch.add(Calendar.DAY_OF_YEAR,off[2]);
			beginBatch.getTime();
			break;
		case 3:
			beginBatch.add(Calendar.YEAR,1);
			beginBatch.getTime();
			beginBatch.set(Calendar.MONTH, 1);
			beginBatch.getTime();
			beginBatch.add(Calendar.MONTH,off[2]);
			beginBatch.getTime();
			beginBatch.set(Calendar.DAY_OF_MONTH,1);
			beginBatch.getTime();
			beginBatch.add(Calendar.DAY_OF_YEAR,off[3]);
			beginBatch.getTime();
			break;
		}
		return beginBatch;
	}
	
	private Record createRecord(List<VField> fieldList, Date batchUpperBound,FormulaEntity formulaEntity){
		MappingSpace mappingSpace = null;
		Record record=new Record();
		Field f=null;
		String name = null;
		Indicator indicator = null;
		Element element = null;
		Calendar upperBound = this.batchMapping.calcUpperBound(formulaEntity.getCycle(), batchUpperBound);
		
		for(int i =0;i<fieldList.size();i++){
			//获取数据
			f = (Field)fieldList.get(i);
			name = f.getName();
			if(name.charAt(0) == 'i'){
				indicator = this.dao.getIndicator(name);
				if(indicator ==null){
					ServiceContext.getInstance().getLogger().info("failed to get indicator---indicatorId:"+name);
					return null;
				}
				
				Calendar begin = Calendar.getInstance();
				begin.setTime( Engine.batchMap.get(name).getTime() );
				mappingSpace = this.batchMapping.getIMappingBatch(indicator.getCycle(), indicator.getCycleUnit(), begin, upperBound);
				
				element = this.dao.batchData(name, mappingSpace.getBegin(), mappingSpace.getEnd());
				if(element == null){
					ServiceContext.getInstance().getLogger().info("failed to get element data---indiactorId: "+name+"--beginDate: "+mappingSpace.getBegin()+"--endDate: "+mappingSpace.getEnd());
					return null;
				}
				f.setValue(element.getValue());
				
				record.addField(name, f);
			}else{
				Calendar begin = Calendar.getInstance();
				begin.setTime( Engine.batchMap.get(name).getTime() );
				mappingSpace = this.batchMapping.getTMappingBatch(begin,upperBound,getTargetCycle(name));
				
				element = this.dao.batchData(name, mappingSpace.getBegin(), mappingSpace.getEnd());
				if(element == null){
					ServiceContext.getInstance().getLogger().info("failed to get element data---targetId:"+name+"--beginDate: "+mappingSpace.getBegin()+"--endDate: "+mappingSpace.getEnd());
					return null;
				}
				f.setValue(element.getValue());
				
				record.addField(name, f);
			}
		}
		return record;
	}
	
	public boolean formulaCheck(String targetId,String formula){
		
		if(this.dao == null){
			try {
				this.dao = SPMDaoFactory.getInstance().getQueryDao();
			} catch (ReflectiveOperationException e) {
				throw new RuntimeException("failed to get queryDao",e);
			}
		}
		
		Target target = this.dao.getTarget(targetId);
		if(target == null)
			return false;
		
		//读取target的所有子目标和子指标，利用公式，创建虚拟表
		Table table = new Table(targetId);
		String id;
		TYPE type;
		
		List<Indicator> indicatorList = target.getSubIndicators();
		if(indicatorList != null){
			for(Indicator i : indicatorList){
				id = i.getId();
				if(i.getType() == 1){
					type = TYPE.INT;
				}else{
					type = TYPE.DOUBLE;
				}
				table.addField(id, new Field(id, type));
			}
		}
		
		List<Target> targetList = target.getSubTargets();
		if(targetList != null){
			for(Target t:targetList){
				id = t.getId();
				if(t.getType() == 1){
					type = TYPE.INT;
				}else{
					type =TYPE.DOUBLE;
				}
				table.addField(id, new Field(id,type));
			}
		}//建表完成
		
		//检验公式
		@SuppressWarnings("unused")
		CherryParser parser = null;
		try {
			parser = CherryEngine.getInstance().parse(table, formula);
		} catch (com.zxbts.engine.cherry.javacc.ParseException e) {
			return false;//捕获到异常，直接返回false
		}
		return true;
	}
}
