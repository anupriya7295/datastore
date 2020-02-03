package com.datastore.service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

public final class DataStoreUtil {

	private DataStoreUtil() {
	}

	public static boolean checkIfKeyExist(String key) {
		Map<String, Integer> value = getMapByFileName(DataStoreService.getDataStore().getDataStoreLineNumberFilepath());
		return value != null && value.get(key) != null ? true : false;
	}

	public static boolean checkIfKeyDeactivated(String key) {
		File file = new File(DataStoreService.getDataStore().getDataStoreDeactivatedkeyFilePath());
		if (!file.exists())
			return false;
		Path path = Paths.get(DataStoreService.getDataStoreDeactivatedkeyFilePath());
		List<String> lines;
		try {
			lines = Files.readAllLines(path, StandardCharsets.UTF_8);
			if (lines.size() !=0 && lines.get(0) != "") {
				String[] keys = lines.get(0).split(",");
				List<String> list = Arrays.asList(keys);
				return list.contains(key);
			}
		} catch (IOException e) {
			System.out.println("Error while checking deactivated key");
		}
		return false;
	}

	public static int getLinenumberByKey(String key) {
		Map<String, Integer> value = getMapByFileName(DataStoreService.getDataStore().getDataStoreLineNumberFilepath());
		return value != null && value.get(key) != null ? value.get(key) : -1;
	}

	public static boolean validateKey(String key) {
		int len = key.split("").length;
		return len > 0 && len <= 32;
	}

	public static boolean validateValue(Map<String, Object> value) {
		return (value.toString().getBytes().length <= 15000) ? true : false;
	}

	public static boolean checkFileSize() {
		File file = new File(DataStoreService.getDataStorePath());
		return (file.length() <= 1000000000) ? true : false;
	}

	public static Map<String, Integer> getMapByFileName(String path) {
		Type type = new TypeToken<Map<String, Integer>>() {
		}.getType();
		Map<String, Integer> value = null;
		File file = new File(path);
		Gson gson = new Gson();
		if (file.exists()) {
			try {
				value = gson.fromJson(new FileReader(path), type);
			} catch (JsonIOException | JsonSyntaxException | FileNotFoundException e) {
				System.out.println("Error while opening file in getMapByFileName");
			}
		}
		return value;
	}

	public static void storeLineNumber(String key) {
		FileWriter fileWriter = null;
		try {
			int linenumber = getlastLineNumber();
			Map<String, Integer> data = getMapByFileName(
					DataStoreService.getDataStore().getDataStoreLineNumberFilepath());
			if (data == null) {
				data = new HashMap<>();
			}
			data.put(key, linenumber);
			fileWriter = new FileWriter(DataStoreService.getDataStore().getDataStoreLineNumberFilepath());
			fileWriter.write(data.toString());
			fileWriter.flush();
			fileWriter.close();
		} catch (IOException e) {
			System.out.println("Error while getting line number");
		}
	}

	public static void removeLineNumberFromLineNumberMap(String key) {
		FileWriter fileWriter = null;
		try {
			Map<String, Integer> data = getMapByFileName(
					DataStoreService.getDataStore().getDataStoreLineNumberFilepath());
			data.remove(key);
			fileWriter = new FileWriter(DataStoreService.getDataStoreLineNumberFilepath());
			fileWriter.write(data.toString());
			fileWriter.flush();
			fileWriter.close();
		} catch (IOException e) {
			System.out.println("Error while getting line number in removeLineNumberFromLineNumberMap");
		}
	}

	public static int getlastLineNumber() {
		LineNumberReader lr = null;
		try {
			lr = new LineNumberReader(new FileReader(DataStoreService.getDataStore().getDataStorePath()));
			int linenumber = 0;
			while (lr.readLine() != null) {
				linenumber++;
			}
			lr.close();
			return linenumber;
		} catch (IOException e) {
			System.out.println("Error while getting last line number");
		}
		return -1;
	}

}
