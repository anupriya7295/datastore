package com.datastore.service;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.JsonParser;

public class DataStoreService {

	public static DataStoreService service = null;
	public static String userHomePath;
	public FileLock lock = null;

	private DataStoreService() {
	}

	public static String getDataStorePath() {
		return userHomePath + "/datastore.txt";
	}

	public static String getDataStoreLineNumberFilepath() {
		return userHomePath + "/datastorelinemap.json";
	}

	public static String getDataStoreDeactivatedkeyFilePath() {
		return userHomePath + "/datastoredeactivatedkey.json";
	}

	public static DataStoreService getDataStore() {
		if (null == service) {
			service = new DataStoreService();
			userHomePath = System.getProperty("user.home");
		}
		return service;
	}

	public static DataStoreService getDataStore(String path) {
		if (null == service) {
			service = new DataStoreService();
			userHomePath = path;
		}
		return service;
	}

	public synchronized Object createKey(String key, Map<String, Object> value, int expiry) {
		if (DataStoreUtil.checkIfKeyDeactivated(key))
			return "Key is Deactivated";
		if (!DataStoreUtil.checkFileSize())
			return "File Size exceed";
		if (!DataStoreUtil.validateKey(key))
			return "Invalid Key";
		if (!DataStoreUtil.validateValue(value))
			return "Invalid Value";
		if (DataStoreUtil.checkIfKeyExist(key))
			return "Key already Exist";
		FileWriter fileWriter = null;
		try {
			Gson gson = new Gson();
			String data = key + "=" + gson.toJson(value);
			fileWriter = new FileWriter(getDataStorePath(), true);
			fileWriter.write(data);
			fileWriter.write('\n');
			fileWriter.flush();
			fileWriter.close();
			if (expiry != -1) {
				new TimeToLiveService(key, expiry).start();
			}
			DataStoreUtil.storeLineNumber(key);
			return value;
		} catch (Exception e) {
			return "Error while indexing in data store" + e;
		}

	}

	public synchronized Object retreiveKey(String key) {
		if (DataStoreUtil.checkIfKeyDeactivated(key))
			return "Key is Deactivated";
		if (!DataStoreUtil.checkIfKeyExist(key))
			return "Key not exist";
		int linenumber = DataStoreUtil.getLinenumberByKey(key);
		try (BufferedReader reader = Files.newBufferedReader(
				Paths.get(getDataStorePath()), StandardCharsets.UTF_8)) {
			List<String> line = reader.lines().skip(linenumber - 1).limit(1).collect(Collectors.toList());
			Gson gson = new Gson();
			return gson.toJson(JsonParser.parseString(line.get(0).replaceFirst(key + "=", "")));
		} catch (IOException e) {
			return "Error while getting key";
		}
	}

	public synchronized String deleteKey(String key) {
		if (DataStoreUtil.checkIfKeyDeactivated(key))
			return "Key is Deactivated";
		if (!DataStoreUtil.checkIfKeyExist(key))
			return "Key not exist";
		int lineNumberTobeDeleted = DataStoreUtil.getLinenumberByKey(key);
		Path path = Paths.get(getDataStorePath());
		List<String> lines;
		try {
			lines = Files.readAllLines(path, StandardCharsets.UTF_8);
			lines.set(lineNumberTobeDeleted - 1, "Deleted");
			Files.write(path, lines, StandardCharsets.UTF_8);
			DataStoreUtil.removeLineNumberFromLineNumberMap(key);
			return key + " Deleted";
		} catch (IOException e) {
			return "Error while deleting key" + key;
		}
	}

	public synchronized void deactivateKey(String key) {
		FileWriter fileWriter = null;
			try {
				fileWriter = new FileWriter(getDataStoreDeactivatedkeyFilePath(), true);
				fileWriter.write(key);
				fileWriter.write(',');
				fileWriter.flush();
				fileWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

}
