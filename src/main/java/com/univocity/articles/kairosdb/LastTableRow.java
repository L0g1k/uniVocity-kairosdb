package com.univocity.articles.kairosdb;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import org.apache.commons.io.IOUtils;

public class LastTableRow {

	private final Optional<Long> lastRowId;

	public LastTableRow() {
		this.lastRowId = this.resolveLastRowId();
	}
	
	private Optional<Long> resolveLastRowId() {
		String pathString = "last_id.txt";
		Path path = Paths.get(pathString);
		String lastId = null;
		try {
			if(!Files.exists(path)) {
				Files.createFile(path);
			}
			File file = new File(pathString);
			
			lastId = IOUtils.toString(new FileInputStream(file));
		} catch (Exception e) {
			return Optional.empty();
		}
		
		if(lastId == null || lastId.isEmpty()) {
			return Optional.empty();
		} else {
			try {
				return Optional.of((Long.parseLong(lastId)));
			} catch (Exception e) {
				e.printStackTrace();
				return Optional.empty();
			}
		}
	}

	public Optional<Long> getLastRowId() {
		return lastRowId;
	}
}
