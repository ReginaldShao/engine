package com.zxbts.service.spm.engine;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.zxbts.frame.ServiceContext;
import com.zxbts.service.spm.model.DataUnit;
import com.zxbts.service.spm.model.QueryDao;
import com.zxbts.service.spm.model.SPMDaoFactory;

public class EngineThread{
	private static int THREAD_POOL_SIZE = 5;
	private static long SLEEP_TIME = 5000;
	private static ExecutorService exec = Executors.newFixedThreadPool(THREAD_POOL_SIZE);;
	private static QueryDao dao;
	
	public EngineThread() {
	}
	
	public static void main(String args[]){
		try {
			ServiceContext.getInstance().initialize(args);
		} catch (RuntimeException e) {
			e.printStackTrace();
		} catch (ReflectiveOperationException e) {
			e.printStackTrace();
		}
		EngineThread engineThread = new EngineThread();
		engineThread.start();
	}
	
	public void start(){
		this.init();
		this.startService();
	}
	private void init(){
		if(EngineThread.dao == null){
			try {
				EngineThread.dao = SPMDaoFactory.getInstance().getQueryDao();
			} catch (ReflectiveOperationException e) {
				throw new RuntimeException("failed to getQueryDao!"+e);
			}
		}
	}
	private void startService(){
		List<DataUnit> dataList = null;
		Iterator<DataUnit> iter = null;
		DataUnit dataUnit = null;
		
		while(true){
			dataList = EngineThread.dao.getData();
			while(dataList != null && dataList.size() >0){
				iter = dataList.iterator();
				while(iter.hasNext()){
					dataUnit = iter.next();
					exec.execute(new EngineRunnable(dataUnit));
				}
				
				dataList = EngineThread.dao.getData();
			}
			
			try {
				Thread.currentThread();
				ServiceContext.getInstance().getLogger().info(Thread.currentThread().getName()+" sleep.....");
				Thread.sleep(SLEEP_TIME);
				ServiceContext.getInstance().getLogger().info(Thread.currentThread().getName()+" wakeup )-_-( ");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}

class EngineRunnable implements Runnable{

	private Engine engine;
	EngineRunnable(DataUnit dataUnit){
		this.engine = new Engine(dataUnit);
	}
	
	@Override
	public void run() {
		this.engine.calcAndWrite();
	}
}
