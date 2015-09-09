/*
 * Copyright © WebServices pour l'Éducation, 2014
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.wseduc.gridfs;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.mongodb.*;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.file.FileSystem;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import com.mongodb.util.JSON;

import javax.net.ssl.SSLSocketFactory;

public class GridFSPersistor extends BusModBase implements Handler<Message<Buffer>> {

	public static final String CHUNKS = ".chunks";
	public static final String FILES = ".files";
	protected String address;
	protected String host;
	protected int port;
	protected String dbName;
	protected String username;
	protected String password;
	protected String bucket;

	protected Mongo mongo;
	protected DB db;

	public void start() {
		super.start();

		address = getOptionalStringConfig("address", "vertx.gridfspersistor");

		host = getOptionalStringConfig("host", "localhost");
		port = getOptionalIntConfig("port", 27017);
		dbName = getOptionalStringConfig("db_name", "default_db");
		username = getOptionalStringConfig("username", null);
		password = getOptionalStringConfig("password", null);
		ReadPreference readPreference = ReadPreference.valueOf(
				getOptionalStringConfig("read_preference", "primary"));
		int poolSize = getOptionalIntConfig("pool_size", 10);
		boolean autoConnectRetry = getOptionalBooleanConfig("auto_connect_retry", true);
		int socketTimeout = getOptionalIntConfig("socket_timeout", 60000);
		boolean useSSL = getOptionalBooleanConfig("use_ssl", false);
		JsonArray seedsProperty = config.getArray("seeds");
		bucket = getOptionalStringConfig("bucket", "fs");

		try {
			MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
			builder.connectionsPerHost(poolSize);
			builder.autoConnectRetry(autoConnectRetry);
			builder.socketTimeout(socketTimeout);
			builder.readPreference(readPreference);

			if (useSSL) {
				builder.socketFactory(SSLSocketFactory.getDefault());
			}

			if (seedsProperty == null) {
				ServerAddress address = new ServerAddress(host, port);
				mongo = new MongoClient(address, builder.build());
			} else {
				List<ServerAddress> seeds = makeSeeds(seedsProperty);
				mongo = new MongoClient(seeds, builder.build());
			}

			db = mongo.getDB(dbName);
			if (username != null && password != null) {
				db.authenticate(username, password.toCharArray());
			}
		} catch (UnknownHostException e) {
			logger.error("Failed to connect to mongo server", e);
		}
		eb.registerHandler(address, this);
		eb.registerHandler(address + ".json", new Handler<Message<JsonObject>>() {
			@Override
			public void handle(Message<JsonObject> message) {
				String action = message.body().getString("action", "");
				switch (action) {
					case "write" :
						writeTo(message);
						break;
					default:
						sendError(message, "Invalid action");
				}
			}
		});
	}

	private List<ServerAddress> makeSeeds(JsonArray seedsProperty) throws UnknownHostException {
		List<ServerAddress> seeds = new ArrayList<>();
		for (Object elem : seedsProperty) {
			JsonObject address = (JsonObject) elem;
			String host = address.getString("host");
			int port = address.getInteger("port");
			seeds.add(new ServerAddress(host, port));
		}
		return seeds;
	}

	private void writeTo(Message<JsonObject> message) {
		String path = message.body().getString("path");
		if (path == null) {
			sendError(message, "Invalid output path.");
			return;
		}
		JsonObject query = message.body().getObject("query");
		if (query == null) {
			sendError(message, "Invalid query.");
			return;
		}
		JsonObject alias = message.body().getObject("alias", new JsonObject());
		boolean renameIfExists = message.body().getBoolean("rename-if-exists", true);
		GridFS fs = new GridFS(db, bucket);
		try {
			List<GridFSDBFile> files = fs.find(jsonToDBObject(query));
			FileSystem fileSystem = vertx.fileSystem();
			for (GridFSDBFile f : files) {
				String a = alias.getString(f.getId().toString());
				String p = path + File.separator + ((a != null) ? a : f.getFilename());
				if (renameIfExists &&  fileSystem.existsSync(p)) {
					p = path + File.separator + f.getId().toString() + "_" +
							((a != null) ? a : f.getFilename());
				}
				f.writeTo(p);
			}
			sendOK(message, new JsonObject().putNumber("number", files.size()));
		} catch (IOException | MongoException e) {
			logger.error(e.getMessage(), e);
			sendError(message, e.getMessage());
		}
	}

	public void stop() {
		mongo.close();
	}

	@Override
	public void handle(Message<Buffer> message) {
		if (message.body() != null) {
			Buffer content = message.body();
			int headerSize = content.getInt(content.length() - 4);
			byte [] header = content.getBytes(content.length() - 4 - headerSize, content.length() - 4);
			JsonObject json = new JsonObject();
			try {
				json = new JsonObject(new String(header, "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				container.logger().error(e.getMessage(), e);
			}
			byte [] data = content.getBytes(0, content.length() - 4 - headerSize);

			switch (json.getString("action")) {
			case "save":
				persistFile(message, data, json);
				break;
			case "saveChunk":
				persistChunk(message, data, json);
				break;
			case "getChunk":
				getChunk(message, json);
				break;
			case "countChunks":
				countChunks(message, json);
				break;
			case "findone":
				getFile(message, json);
				break;
			case "remove":
				removeFile(message, json);
				break;
			case "copy":
				copyFile(message, json);
				break;
			default:
				replyError(message, "Invalid message");
				break;
			}
		} else {
			replyError(message, "Invalid message");
		}
	}

	private void persistChunk(Message<Buffer> message, byte[] data, JsonObject json) {
		final Integer n = json.getInteger("n");
		if (n == null) {
			replyError(message, "Invalid chunk number");
			return;
		}
		final String id = json.getString("_id");
		if (id == null || id.trim().isEmpty()) {
			replyError(message, "Invalid file id");
			return;
		}
		if (n == 0) {
			try {
				saveFile(json, data.length);
			} catch (MongoException e) {
				logger.error(e.getMessage(), e);
				replyError(message, e.getMessage());
				return;
			}
		}
		try {
			DBObject chunk = BasicDBObjectBuilder.start()
					.add("files_id", id)
					.add("n", n)
					.add("data", data).get();
			DBCollection collection = db.getCollection(bucket + CHUNKS);
			collection.save(chunk);
			replyOK(message, new JsonObject());
		} catch (RuntimeException e) {
			logger.error(e.getMessage(), e);
			replyError(message, e.getMessage());
		}
	}

	private void saveFile(JsonObject json, long chunkSize) {
		json.putNumber("chunkSize", chunkSize);
		json.removeField("n");
		if (json.getString("content-type") != null) {
			json.putString("contentType", json.getString("content-type"));
			json.removeField("content-type");
		}
		DBObject file = jsonToDBObject(json);
		file.put("uploadDate", new Date());
		DBCollection collection = db.getCollection(bucket + FILES);
		collection.save(file);
	}

	private void countChunks(Message<Buffer> message, JsonObject json) {
		String filesId = json.getString("files_id");
		if (filesId == null || filesId.trim().isEmpty()) {
			replyError(message, "Invalid file id.");
			return;
		}
		try {
			DBObject file = BasicDBObjectBuilder.start("files_id", filesId).get();
			DBCollection collection = db.getCollection(bucket + CHUNKS);
			long count = collection.count(file);
			message.reply(count);
		} catch (RuntimeException e) {
			logger.error(e.getMessage(), e);
			replyError(message, e.getMessage());
		}
	}

	private void getChunk(Message<Buffer> message, JsonObject json) {
		String filesId = json.getString("files_id");
		if (filesId == null || filesId.trim().isEmpty()) {
			replyError(message, "Invalid file id.");
			return;
		}
		Long n = json.getLong("n");
		if (n == null) {
			replyError(message, "Invalid chunk number.");
			return;
		}
		DBCollection collection = db.getCollection(bucket + CHUNKS);
		DBObject chunk = collection.findOne(BasicDBObjectBuilder
				.start("files_id", filesId).add("n", n).get());
		if (chunk == null) {
			replyError(message, "Error getting chunk number " + n + " in file  " + filesId);
			return;
		}
		logger.debug("chunk : " + chunk);
		message.reply(new Buffer((byte[]) chunk.get("data")));
	}

	private void getFile(Message<Buffer> message, JsonObject json) {
		JsonObject query = json.getObject("query");
		if (query == null) {
			return;
		}
		GridFS fs = new GridFS(db, bucket);
		try {
			GridFSDBFile f = fs.findOne(jsonToDBObject(query));
			if (f == null) {
				replyError(message, "File not found with query : " + query.encode());
				return;
			}
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			f.writeTo(os);
			message.reply(new Buffer(os.toByteArray()));
		} catch (IOException | MongoException e) {
			container.logger().error(e.getMessage(), e);
			JsonObject j = new JsonObject().putString("status", "error").putString("message", e.getMessage());
			try {
				message.reply(new Buffer(j.encode().getBytes("UTF-8")));
			} catch (UnsupportedEncodingException e1) {
				container.logger().error(e1.getMessage(), e1);
			}
		}
	}

	private void persistFile(Message<Buffer> message, byte[] data, JsonObject header) {
		GridFS fs = new GridFS(db, bucket);
		GridFSInputFile f = fs.createFile(data);
		String id = header.getString("_id");
		if (id == null || id.trim().isEmpty()) {
			id = UUID.randomUUID().toString();
		}
		f.setId(id);
		f.setContentType(header.getString("content-type"));
		f.setFilename(header.getString("filename"));
		f.save();
		JsonObject reply = new JsonObject();
		reply.putString("_id", id);
		replyOK(message, reply);
	}

	private void copyFile(Message<Buffer> message, JsonObject json) {
		JsonObject query = json.getObject("query");
		if (query == null) {
			return;
		}
		GridFS fs = new GridFS(db, bucket);
		try {
			GridFSDBFile f = fs.findOne(jsonToDBObject(query));
			if (f == null) {
				replyError(message, "File not found with query : " + query.encode());
				return;
			}
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			f.writeTo(os);
			JsonObject j = new JsonObject();
			j.putString("content-type", f.getContentType());
			j.putString("filename", f.getFilename());
			persistFile(message, os.toByteArray(), j);
		} catch (IOException | MongoException e) {
			replyError(message, e.getMessage());
		}
	}

	private void removeFile(Message<Buffer> message, JsonObject json) {
		JsonObject query = json.getObject("query");
		if (query == null) {
			return;
		}
		GridFS fs = new GridFS(db, bucket);
		try {
			fs.remove(jsonToDBObject(query));
			replyOK(message, null);
		} catch (MongoException e) {
			replyError(message, e.getMessage());
		}
	}

	private DBObject jsonToDBObject(JsonObject object) {
		String str = object.encode();
		return (DBObject)JSON.parse(str);
	}

	protected void replyOK(Message<Buffer> message, JsonObject reply) {
		if (reply == null) {
			reply = new JsonObject();
		}
		reply.putString("status", "ok");
		message.reply(reply);
	}

	protected void replyError(Message<Buffer> message, String error) {
		logger.error(error);
		JsonObject json = new JsonObject().putString("status", "error").putString("message", error);
		message.reply(json);
	}
}
