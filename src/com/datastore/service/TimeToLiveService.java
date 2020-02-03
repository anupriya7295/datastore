package com.datastore.service;

public class TimeToLiveService extends Thread {
	private String key;
	private int expiry;
	
	public TimeToLiveService(String key, int expiry) {
		this.key = key;
		this.expiry = expiry;
	}
	
	@Override
	public void run() {
		System.out.println(this.key + " is set Time to live for " + this.expiry + " seconds");
		try {
			Thread.sleep(this.expiry * 1000);
			DataStoreService.getDataStore().deactivateKey(key);
		} catch (InterruptedException e) {
			System.out.println("Error in time to live thread execution "+key);
		}
		

	}
}

