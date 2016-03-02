package com.zxbts.service.spm.engine;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class BatchMapping {
	public static String PATTERN = "yyyyMMdd";
	public static class MappingSpace{
		private String begin;
		private String end;
		
		public MappingSpace(){}
		
		public MappingSpace(String begin, String end) {
			this.begin = begin;
			this.end = end;
		}
		public MappingSpace(Calendar begin,Calendar end){
			this.begin = BatchMapping.calendar2String(begin, BatchMapping.PATTERN);
			this.end = BatchMapping.calendar2String(end, BatchMapping.PATTERN);
		}
		public String getBegin() {
			return begin;
		}
		
		public void setBegin(String begin) {
			this.begin = begin;
		}
		
		public String getEnd() {
			return end;
		}
		
		public void setEnd(String end) {
			this.end = end;
		}
	}
	
	public BatchMapping(){
	}
	
	public BatchMapping(MappingSpace parent){
	}
	
	//传入更新了数据的indicator的相关参数，计算对于父target的影响域。
	public MappingSpace getMappingBatch(int cycle,int cycle_unit,String batchNo) {
		Calendar begin_calendar = string2Calendar(batchNo, BatchMapping.PATTERN);
		Calendar end_calendar = string2Calendar(batchNo, BatchMapping.PATTERN);
		
		switch(cycle_unit){
		case 1:
			end_calendar.add(Calendar.DAY_OF_YEAR, cycle);//加上一个采集周期的时间
			break;
		case 2:
			end_calendar.add(Calendar.MONTH, cycle);
			break;
		case 3:
			end_calendar.add(Calendar.YEAR, cycle);
			break;
		}
		begin_calendar.getTime();
		end_calendar.getTime();
		return new MappingSpace(begin_calendar,end_calendar);
	}
	
	public MappingSpace getIMappingBatch(int cycle,int cycle_unit,Calendar begin,Calendar upperBound){
		switch(cycle_unit){
		case 1:
			while(true){
				begin.add(Calendar.DAY_OF_YEAR, cycle);
				if(begin.after(upperBound)){
					begin.add(Calendar.DAY_OF_YEAR, -cycle);
					break;
				}
			}
			break;
		case 2:
			while(true){
				begin.add(Calendar.MONTH, cycle);
				if(begin.after(upperBound)){
					begin.add(Calendar.MONTH, -cycle);
					break;
				}
			}
			break;
		case 3:
			while(true){
				begin.add(Calendar.YEAR, cycle);
				if(begin.after(upperBound)){
					begin.add(Calendar.YEAR, -cycle);
					break;
				}
			}
			break;
		}
		begin.getTime();
		upperBound.getTime();
		return new MappingSpace(begin,upperBound);
	}
	
	public MappingSpace getTMappingBatch(Calendar begin,Calendar upperBound,int cycle_unit){
		switch(cycle_unit){
		case 1:
			while(true){
				begin.add(Calendar.DAY_OF_YEAR, 1);
				if(begin.after(upperBound)){
					begin.add(Calendar.DAY_OF_YEAR, -1);
					break;
				}
			}
			break;
		case 2:
			while(true){
				begin.add(Calendar.MONTH, 1);
				if(begin.after(upperBound)){
					begin.add(Calendar.MONTH, -1);
					break;
				}
			}
			break;
		case 3:
			while(true){
				begin.add(Calendar.YEAR, 1);
				if(begin.after(upperBound)){
					begin.add(Calendar.YEAR, -1);
					break;
				}
			}
			break;
		}
		begin.getTime();
		upperBound.getTime();
		return new MappingSpace(begin,upperBound);
	}
	
	public static int[] parseOffset(String offset){
		int[] res ={0,0,0}; 
		int inxY = offset.indexOf('Y');
		int inxM = offset.indexOf('M');
		int inxD = offset.indexOf('D');
		
		if(inxY != -1){
			res[0]= offset.charAt(inxY-1)-'0';
		}
		if(inxM != -1){
			res[1]= offset.charAt(inxM-1)-'0';
		}
		if(inxD != -1){
			res[2]= offset.charAt(inxD-1)-'0';
		}
		return res;
	}
	
	public Calendar calcUpperBound(int target_cycle,Date batch){
		Calendar upperBound = Calendar.getInstance();
		
		upperBound.setTime(batch);
		switch(target_cycle){
		case 1:
			upperBound.add(Calendar.DAY_OF_YEAR, 1);
			upperBound.getTime();
			break;
		case 2:
			upperBound.add(Calendar.MONTH, 1);
			upperBound.getTime();
			break;
		case 3:
			upperBound.add(Calendar.YEAR, 1);
			upperBound.getTime();
			break;
		}
		upperBound.add(Calendar.DAY_OF_YEAR,-1);
		upperBound.getTime();
		return upperBound;
	}
	
	public static String calendar2String(Calendar calendar, String pattern){
		DateFormat dateFormat = new SimpleDateFormat(pattern);
		return dateFormat.format(calendar.getTime());
	}
	
	public static Calendar string2Calendar(String time,String pattern){
		Calendar calendar = Calendar.getInstance();
		DateFormat dateFormat = new SimpleDateFormat(pattern);
		Date date = null;
		try {
			date = dateFormat.parse(time);
			calendar.setTime(date);
		} catch (ParseException e) {
			throw new RuntimeException("parse failed!"+e);
		}
		return calendar;
	}
}