/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Map;

import java.util.Vector;
import java.util.LinkedList;
import java.util.Queue;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public final class RedisClient extends DB {

  private Jedis jedis;

  //for nvm-test - by bigs
  private Pipeline p;
  private boolean isPipelined;
  private long pipelinedOps;
  private long pipelineLimit; // if exceed this, send pipeline
  private Queue<Status> statusesUpdate;
  private Queue<Status> statusesRead;

  private Queue<Response<List<String>>> resHmget;
  private Queue<Response<Map<String, String>>> resHgetAll;
  private Queue<Response<String>> resHmset;
  
  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String PIPELINE_PROPERTY = "redis.pipeline";

  public static final String INDEX_KEY = "_indices";

  public void init() throws DBException {
    //System.out.println("[bigs] jedis - init");
    Properties props = getProperties();
    int port;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = Protocol.DEFAULT_PORT;
    }
    String host = props.getProperty(HOST_PROPERTY);
    String pipelineString = props.getProperty(PIPELINE_PROPERTY);
    if(pipelineString != null){
      pipelineLimit = Integer.parseInt(pipelineString);
      if(pipelineLimit > 1 || pipelineLimit < 0){
        isPipelined = true;
      }
    }

    jedis = new Jedis(host, port);
    jedis.connect();
    
    String password = props.getProperty(PASSWORD_PROPERTY);
    if (password != null) {
      jedis.auth(password);
    }
//    isPipelined = true; //TODO deafult for test
//    pipelineLimit = 1000;
    if(isPipelined){
      p = jedis.pipelined();
      statusesUpdate = new LinkedList<Status>();
      statusesRead = new LinkedList<Status>();
      resHmget = new LinkedList<Response<List<String>>>();
      resHgetAll = new LinkedList<Response<Map<String, String>>>();
      resHmset = new LinkedList<Response<String>>();
    }
  }

  public void cleanup() throws DBException {
    if(isPipelined){
      if(pipelinedOps > 0){
        syncPipeline();
      }
      
      int cntReadOK = 0;
      int cntUpdateOK = 0;
      for(Status status : statusesRead){
        if(status.equals(Status.OK)){
          cntReadOK++;
        }
      }
      for(Status status : statusesUpdate){
        if(status.equals(Status.OK)){
          cntUpdateOK++;
        }
      }
      statusesRead.clear();
      statusesUpdate.clear();
      System.out.println("[Pipeline] Read OK : "+ cntReadOK +" Update OK : "+ cntUpdateOK);
    }
    jedis.disconnect();
    //System.out.println("[bigs] jedis - cleanup");
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`


  private void syncPipeline(){
    long st = System.currentTimeMillis();
    p.sync();
    long en = System.currentTimeMillis();
    //response check and accumulate status result
    //read
    System.out.println("syncPipeline : sync time : "+(en-st));
    for (Response<Map<String, String>> res : resHgetAll){
      statusesRead.add(res.get().isEmpty() ? Status.ERROR : Status.OK);
    }
    resHgetAll.clear();
    for (Response<List<String>> res : resHmget){
      statusesRead.add(res.get().isEmpty() ? Status.ERROR : Status.OK);
    }
    resHmget.clear();
    //update
    for (Response<String> res : resHmset){
      statusesUpdate.add(res.get().equals("OK") ? Status.OK : Status.ERROR);
    }
    resHmset.clear();

    pipelinedOps = 0;
    p = jedis.pipelined();
  }

  private void pipelineProcess(){
    pipelinedOps++;
    if (pipelineLimit < 0){ //pipeline all
      return; 
    }
    if (pipelinedOps >= pipelineLimit){
      syncPipeline();
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    //System.out.println("[bigs] jedis - read");
    if (isPipelined){
      if (fields == null){
//        resHgetAll.add(p.hgetAll(key));
        p.hgetAll(key);
      }else{
        String[] fieldArray =
            (String[]) fields.toArray(new String[fields.size()]);
       // resHmget.add(p.hmget(key, fieldArray));
        p.hmget(key, fieldArray);
      }
      pipelineProcess();
      return Status.OK;
    }

    if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
    } else {
      String[] fieldArray =
          (String[]) fields.toArray(new String[fields.size()]);
      List<String> values = jedis.hmget(key, fieldArray);
      Iterator<String> fieldIterator = fields.iterator();
      Iterator<String> valueIterator = values.iterator();

      while (fieldIterator.hasNext() && valueIterator.hasNext()) {
        String field = fieldIterator.next();
        String value = valueIterator.next();
        result.put(field,
            new StringByteIterator(value));
      }
      assert !fieldIterator.hasNext() && !valueIterator.hasNext();
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    //System.out.println("[bigs] jedis - insert");
    if (jedis.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK")) {
      jedis.zadd(INDEX_KEY, hash(key), key);
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    //System.out.println("[bigs] jedis - delete");
    return jedis.del(key) == 0 && jedis.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
        : Status.OK;
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    //System.out.println("[bigs] jedis - update");
    if (isPipelined){
      //resHmset.add(p.hmset(key, StringByteIterator.getStringMap(values)));
      p.hmset(key, StringByteIterator.getStringMap(values));
      pipelineProcess();
      return Status.OK;
    }
    return jedis.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    //System.out.println("[bigs] jedis - scan");
    Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
        Double.POSITIVE_INFINITY, 0, recordcount);

    HashMap<String, ByteIterator> values;
    for (String key : keys) {
      values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
    }

    return Status.OK;
  }

}
