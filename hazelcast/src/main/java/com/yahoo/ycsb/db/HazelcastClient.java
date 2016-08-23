/**
 * Copyright (c) 2013 - 2016 YCSB Contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.db;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

/**
 * from:
 * https://github.com/jasonzhusmile/YCSB_hazelcast_YP/blob/master/db/hazelcast/src/com/yahoo/ycsb/db/HazelcastClient.java
 * 
 * @author ypai
 */
public class HazelcastClient extends DB {

	private final static String CLUSTER_MEMBERS = "hazelcast.members";
	
	private final static String CLUSTER_MEMBERS_DEFAULT = "gridhaz-dt-a1d.ula.comcast.net:5701,gridhaz-dt-a2d.ula.comcast.net:5701,gridhaz-dt-a3d.ula.comcast.net:5701,gridhaz-dt-a4d.ula.comcast.net:5701";

	private final static String GROUP_NAME = "hazelcast.groupName";
	
	private final static String GROUP_NAME_DEFAULT = "dev";

	private final static String GROUP_PASSWORD = "hazelcast.password";

	private static final String GROUP_PASSWORD_DEFAULT  = "dev";
	
	private static final String TABLE = "usertable";

	private HazelcastInstance hazelcastInstance;

	/**
	 * true if ycsb client runs as a client to a Hazelcast cache server.
	 */
	private boolean isClient;

	/*
	 * (non-Javadoc)
	 *
	 * @see com.yahoo.ycsb.DB#init()
	 */
	@Override
	public void init() throws DBException {
		super.init();
		Properties props = getProperties();
		
		hazelcastInstance = com.hazelcast.client.HazelcastClient.newHazelcastClient(clientConfig(props));
		IMap<Object,Object> map = hazelcastInstance.getMap(TABLE);
		map.clear();
	}

	@Override
	public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		Map<String, Map<String, byte[]>> r = hazelcastInstance.getMap(TABLE);
		Map<String, byte[]> val = r.get(key);
		if (val != null) {
			if (fields == null) {
				for (Map.Entry<String, byte[]> entry : val.entrySet()) {
					result.put(entry.getKey(), new ByteArrayByteIterator(entry.getValue()));
				}
			} else {
				for (String field : fields) {
					result.put(field, new ByteArrayByteIterator(val.get(field)));
				}
			}
			return Status.OK;
		}
		return Status.ERROR;
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		// Hazelcast doesn't support scan
		return Status.ERROR;
	}

	@Override
	public Status update(String table, String key, HashMap<String, ByteIterator> values) {
		hazelcastInstance.getMap(TABLE).put(key, convertToBytearrayMap(values));
		return Status.OK;
	}

	@Override
	public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
		hazelcastInstance.getMap(TABLE).put(key, convertToBytearrayMap(values));
		return Status.OK;
	}

	@Override
	public Status delete(String table, String key) {
		hazelcastInstance.getMap(TABLE).delete(key);
		return Status.OK;
	}

	private Map<String, byte[]> convertToBytearrayMap(Map<String, ByteIterator> values) {
		Map<String, byte[]> retVal = new HashMap<String, byte[]>();
		for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
			retVal.put(entry.getKey(), entry.getValue().toArray());
		}
		return retVal;
	}

	public ClientConfig clientConfig(Properties props) {
		String clusterMembers = props.getProperty(CLUSTER_MEMBERS, CLUSTER_MEMBERS_DEFAULT);
		String groupName = props.getProperty(GROUP_NAME, GROUP_NAME_DEFAULT);
		String password = props.getProperty(GROUP_PASSWORD, GROUP_PASSWORD_DEFAULT);
		ClientConfig clientConfig = new ClientConfig();
		List<String> members = Arrays.asList(clusterMembers.split(","));
		clientConfig.getNetworkConfig().setAddresses(members);
		clientConfig.getNetworkConfig().setConnectionAttemptLimit(10);
		clientConfig.getNetworkConfig().setConnectionAttemptPeriod(24 * 60);
		clientConfig.getNetworkConfig().setConnectionTimeout(1000);
		GroupConfig groupConfig = new GroupConfig();
		groupConfig.setName(groupName);
		groupConfig.setPassword(password);
		clientConfig.setGroupConfig(groupConfig);
		return clientConfig;
	}

	/**
	 * Simple logging method.
	 *
	 * @param level
	 * @param message
	 * @param e
	 */
	protected void log(String level, String message, Exception e) {
		message = Thread.currentThread().getName() + ":  " + message;
		System.out.println(message);
		if ("error".equals(level)) {
			System.err.println(message);
		}
		if (e != null) {
			e.printStackTrace(System.out);
			e.printStackTrace(System.err);
		}
	}
}
