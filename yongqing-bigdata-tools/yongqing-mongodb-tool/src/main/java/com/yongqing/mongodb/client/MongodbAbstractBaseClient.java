package com.yongqing.mongodb.client;

import com.mongodb.BasicDBObject;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.*;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.lang.reflect.Field;
import java.util.*;

/**
 *
 */
@Log4j2
public abstract class MongodbAbstractBaseClient implements Client {
    //log
//    private static final Logger logger = LoggerFactory.getLogger(MongodbAbstractClient.class);
    //mongoClient连接

    public void createCollection(String dataBaseName, String collectionName) {
        getDatabase(dataBaseName).createCollection(collectionName);
    }


    public MongoCollection<Document> getCollectionByName(String dataBaseName, String collectionName) {
        return getDatabase(dataBaseName).getCollection(collectionName);
    }

    public FindIterable<Document> findMongoDbDocById(String dataBaseName, String collectionName, String id) {
        BasicDBObject searchDoc = new BasicDBObject().append("_id", id);
        return getCollectionByName(dataBaseName, collectionName).find(searchDoc);
    }

    //按照指定字段查询
    public FindIterable<Document> findMongoDbDocByField(String dataBaseName, String collectionName, String field, String fieldValue, Integer limitNum, String sortField) {
        BasicDBObject searchDoc = new BasicDBObject().append(field, fieldValue);
        return findMongoDbDocByField(dataBaseName, collectionName, searchDoc, limitNum, sortField);
    }

    //按照指定字段查询
    public FindIterable<Document> findMongoDbDocByField(String dataBaseName, String collectionName, Map<String, String> fields, Integer limitNum, String sortField) {
        BasicDBObject searchDoc = new BasicDBObject();
        fields.forEach((k, v) -> {
            searchDoc.append(k, v);
        });
        return findMongoDbDocByField(dataBaseName, collectionName, searchDoc, limitNum, sortField);
    }

    private FindIterable<Document> findMongoDbDocByField(String dataBaseName, String collectionName, BasicDBObject searchDoc, Integer limitNum, String sortField) {
        if (null != limitNum) {
            if (null != sortField) {
                return getCollectionByName(dataBaseName, collectionName).find(searchDoc).sort(new Document().append(sortField, -1)).limit(limitNum);
            } else {
                return getCollectionByName(dataBaseName, collectionName).find(searchDoc).limit(limitNum);
            }
        } else {
            if (null != sortField) {
                return getCollectionByName(dataBaseName, collectionName).find(searchDoc).sort(new Document().append(sortField, -1));
            } else {
                return getCollectionByName(dataBaseName, collectionName).find(searchDoc);
            }
        }
    }

    public FindIterable<Document> findMongoDbDocByField(String dataBaseName, String collectionName, String field, String fieldValue, Integer limitNum) {
        return findMongoDbDocByField(dataBaseName, collectionName, field, fieldValue, limitNum, null);
    }

    public FindIterable<Document> findMongoDbDocByField(String dataBaseName, String collectionName, Map<String, String> fields, Integer limitNum) {
        return findMongoDbDocByField(dataBaseName, collectionName, fields, limitNum, null);
    }

    public FindIterable<Document> findMongoDbDocByFieldRegex(String dataBaseName, String collectionName, String field, String fieldValue, Integer limitNum) {
        return findMongoDbDocByFieldRegex(dataBaseName, collectionName, field, fieldValue, limitNum, null);
    }

    public FindIterable<Document> findMongoDbDocByFieldRegex(String dataBaseName, String collectionName, Map<String, String> fields, Integer limitNum) {
        return findMongoDbDocByFieldRegex(dataBaseName, collectionName, fields, limitNum, null);
    }

    //按照指定字段查询
    public FindIterable<Document> findMongoDbDocByField(String dataBaseName, String collectionName, String field, String fieldValue) {
        return findMongoDbDocByField(dataBaseName, collectionName, field, fieldValue, null, null);
    }

    //按照指定字段查询
    public FindIterable<Document> findMongoDbDocByField(String dataBaseName, String collectionName, Map<String, String> fields) {
        return findMongoDbDocByField(dataBaseName, collectionName, fields, null, null);
    }

    //按照指定字段查询
    public FindIterable<Document> findMongoDbDocByField(String dataBaseName, String collectionName, String field, String fieldValue, String sortField) {
        return findMongoDbDocByField(dataBaseName, collectionName, field, fieldValue, null, sortField);
    }

    //按照指定字段查询
    public FindIterable<Document> findMongoDbDocByField(String dataBaseName, String collectionName, Map<String, String> fields, String sortField) {
        return findMongoDbDocByField(dataBaseName, collectionName, fields, null, sortField);
    }

    //按照指定字段模糊查询
    public FindIterable<Document> findMongoDbDocByFieldRegex(String dataBaseName, String collectionName, String field, String fieldValue, Integer limitNum, String sortField) {
        BasicDBObject searchDoc = new BasicDBObject().append(field, new BasicDBObject("$regex", fieldValue));
        return findMongoDbDocByField(dataBaseName, collectionName, searchDoc, limitNum, sortField);
    }


    //按照指定字段模糊查询
    public FindIterable<Document> findMongoDbDocByFieldRegex(String dataBaseName, String collectionName, Map<String, String> fields, Integer limitNum, String sortField) {
        BasicDBObject searchDoc = new BasicDBObject();
        fields.forEach((k, v) -> {
            searchDoc.append(k, new BasicDBObject("$regex", v));
        });
        return findMongoDbDocByField(dataBaseName, collectionName, searchDoc, limitNum, sortField);
    }

    //按照指定字段模糊查询
    public FindIterable<Document> findMongoDbDocByFieldRegex(String dataBaseName, String collectionName, String field, String fieldValue) {
        return findMongoDbDocByFieldRegex(dataBaseName, collectionName, field, fieldValue, null, null);
    }

    //按照指定字段模糊查询
    public FindIterable<Document> findMongoDbDocByFieldRegex(String dataBaseName, String collectionName, Map<String, String> fields) {
        return findMongoDbDocByFieldRegex(dataBaseName, collectionName, fields, null, null);
    }

    //按照指定字段模糊查询
    public FindIterable<Document> findMongoDbDocByFieldRegex(String dataBaseName, String collectionName, String field, String fieldValue, String sortField) {
        return findMongoDbDocByFieldRegex(dataBaseName, collectionName, field, fieldValue, null, sortField);
    }

    //按照指定字段模糊查询
    public FindIterable<Document> findMongoDbDocByFieldRegex(String dataBaseName, String collectionName, Map<String, String> fields, String sortField) {
        return findMongoDbDocByFieldRegex(dataBaseName, collectionName, fields, null, sortField);
    }

    //id模糊查询
    public FindIterable<Document> findMongoDbDocByIdRegex(String dataBaseName, String collectionName, String id) {
        BasicDBObject searchDoc = new BasicDBObject().append("_id", new BasicDBObject("$regex", id));
        return getCollectionByName(dataBaseName, collectionName).find(searchDoc);
    }

    public FindIterable<Document> findMongoDbDocById(String dataBaseName, String collectionName, String startId, String endId) {
        BasicDBObject searchDoc = new BasicDBObject().append("_id", new BasicDBObject("$gte", startId).append("$lte", endId));
        return getCollectionByName(dataBaseName, collectionName).find(searchDoc);
    }

    public FindIterable<Document> findMongoDbDoc(String dataBaseName, String collectionName, BasicDBObject basicDBObject) {
        return getCollectionByName(dataBaseName, collectionName).find(basicDBObject);
    }

    public FindIterable<Document> findMongoDbDoc(String dataBaseName, String collectionName, BasicDBObject basicDBObject, Integer limitNum) {
        return findMongoDbDoc(dataBaseName, collectionName, basicDBObject).limit(limitNum);
    }

    public FindIterable<Document> findMongoDbDocById(String dataBaseName, String collectionName, String startId, String endId, Integer limitNum) {
        return findMongoDbDocById(dataBaseName, collectionName, startId, endId).limit(limitNum);
    }

    /**
     * 降序
     *
     * @param dataBaseName
     * @param collectionName
     * @param startId
     * @param endId
     * @param sortField      排序字段
     * @return
     */
    public FindIterable<Document> findMongoDbDocByIdDescSort(String dataBaseName, String collectionName, String startId, String endId, String sortField) {
        return findMongoDbDocById(dataBaseName, collectionName, startId, endId).sort(new Document().append(sortField, -1));
    }

    public FindIterable<Document> findMongoDbDocByIdDescSort(String dataBaseName, String collectionName, String startId, String endId, String sortField, Integer limitNum) {
        return findMongoDbDocByIdDescSort(dataBaseName, collectionName, startId, endId, sortField).limit(limitNum);
    }

    /**
     * 升序
     *
     * @param dataBaseName
     * @param collectionName
     * @param startId
     * @param endId
     * @param sortField      排序字段
     * @return
     */
    public FindIterable<Document> findMongoDbDocByIdAscSort(String dataBaseName, String collectionName, String startId, String endId, String sortField) {
        return findMongoDbDocById(dataBaseName, collectionName, startId, endId).sort(new Document().append(sortField, 1));
    }

    public FindIterable<Document> findMongoDbDocByIdAscSort(String dataBaseName, String collectionName, String startId, String endId, String sortField, Integer limitNum) {
        return findMongoDbDocByIdAscSort(dataBaseName, collectionName, startId, endId, sortField).limit(limitNum);
    }

    public void insertDoc(String dataBaseName, String collectionName, Document document) {
        getCollectionByName(dataBaseName, collectionName).insertOne(document);
    }

    public void insertDoc(String dataBaseName, String collectionName, List<? extends Document> listData) {
        getCollectionByName(dataBaseName, collectionName).insertMany(listData);
    }

    public void updateDoc(String dataBaseName, String collectionName, Bson var1, Bson var2) {
        getCollectionByName(dataBaseName, collectionName).updateOne(var1, var2);
    }

    public void updateDoc(String dataBaseName, String collectionName, Bson var1, Bson var2, UpdateOptions var3) {
        getCollectionByName(dataBaseName, collectionName).updateOne(var1, var2, var3);
    }

    public void updateDoc(String dataBaseName, String collectionName, Bson var1, Bson var2, Boolean upsert) {
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(upsert);
        updateDoc(dataBaseName, collectionName, var1, var2, updateOptions);
    }

    public <T> void updateDocAndListByPush(String dataBaseName, String collectionName, String searchDocId, T data, Class<T> tclass, Boolean upsert) throws IllegalAccessException {
        BasicDBObject searchDoc = new BasicDBObject();
        searchDoc.append("_id", searchDocId);
        updateDocAndListByPush(dataBaseName, collectionName, searchDoc, data, tclass, upsert);
    }
    public <T> void updateDocAndListBySet(String dataBaseName, String collectionName, String searchDocId, T data, Class<T> tclass, Boolean upsert) throws IllegalAccessException {
        BasicDBObject searchDoc = new BasicDBObject();
        searchDoc.append("_id", searchDocId);
        updateDocAndListBySet(dataBaseName, collectionName, searchDoc, data, tclass, upsert);
    }

    public <T> void updateDocAndListByPush(String dataBaseName, String collectionName, T data, Class<T> tclass, Boolean upsert) throws IllegalAccessException {

        updateDocAndListByPush(dataBaseName, collectionName, (Bson) null, data, tclass, upsert);
    }

    public <T> void updateDocAndListBySet(String dataBaseName, String collectionName, T data, Class<T> tclass, Boolean upsert) throws IllegalAccessException {

        updateDocAndListBySet(dataBaseName, collectionName, (Bson) null, data, tclass, upsert);
    }

    public <T> void updateDocAndListByPush(String dataBaseName, String collectionName, Bson var1, T data, Class<T> tclass, Boolean upsert) throws IllegalAccessException {
        updateDocAndListByPush(dataBaseName,collectionName,var1,data,tclass,upsert,true);
    }

    public <T> void updateDocAndListBySet(String dataBaseName, String collectionName, Bson var1, T data, Class<T> tclass, Boolean upsert) throws IllegalAccessException {
        updateDocAndListByPush(dataBaseName,collectionName,var1,data,tclass,upsert,false);
    }

    private <T> void updateDocAndListByPush(String dataBaseName, String collectionName, Bson var1, T data, Class<T> tclass, Boolean upsert, boolean listByPush) throws IllegalAccessException {
        Field[] fs = tclass.getDeclaredFields();
        if (fs.length == 0) {
            throw new RuntimeException(tclass.getName() + "need define at least one Field");
        }
        String _id = null;
        Map<String, Object> noListData = new HashMap<>();
        Map<String, Object> listData = new HashMap<>();
        for (Field field : fs) {
            field.setAccessible(true);
            log.info("updateDoc fieldType:{}", field);
            if (field.getType().getName().equals("java.lang.String") && field.getName().equals("docId")) {
                _id = (String) field.get(data);
                noListData.put(field.getName(), (String) field.get(data));
            } else if (field.getType().getName().equals("java.lang.String")) {
                noListData.put(field.getName(), (String) field.get(data));
            } else if (field.getType().getName().startsWith("java.util.List") || field.getType().getName().startsWith("java.util.ArrayList") || field.getType().getName().startsWith("java.util.LinkedList")) {
                if (listByPush) {
                    listData.put(field.getName(), objectToMap((List) field.get(data)));
                } else {
                    noListData.put(field.getName(), objectToMap((List) field.get(data)));
                }
            } else {
                throw new RuntimeException(tclass + " do not support field Type" + field.getType().getName());
            }
        }
        if (null == _id && null == var1) {
            throw new RuntimeException(tclass + " need contain field docId and field docId's Type must be String");
        }
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(upsert);
        Document document = new Document();
        if (listData.size() > 0 && listByPush) {
            document.put("$push", listData);
        }
        document.put("$set", noListData);
        if (null == var1) {
            BasicDBObject searchDoc = new BasicDBObject();
            searchDoc.append("_id", _id);
            updateDoc(dataBaseName, collectionName, searchDoc, document, updateOptions);
            return;
        }
        updateDoc(dataBaseName, collectionName, var1, document, updateOptions);
    }


    private List<Map<String, Object>> objectToMap(List<?> list) {
        List<Map<String, Object>> mapList = new ArrayList<>();
        list.forEach(action -> {
            try {
                mapList.add(objectToMap(action));
            } catch (IllegalAccessException e) {
                log.error("objectToMap cause Exception", e);
            }
        });
        return mapList;
    }

    private Map<String, Object> objectToMap(Object obj) throws IllegalAccessException {
        if (obj instanceof Map) {
            return (Map<String, Object>) obj;
        }
        Map<String, Object> map = new HashMap<>();
        Class<?> clazz = obj.getClass();
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            String fieldName = field.getName();
            Object value = field.get(obj);
            map.put(fieldName, value);
        }
        return map;
    }
//
//    public static void main(String[] args) throws IllegalAccessException {
//        Test test = new Test();
//        ArrayList<Map<String, Object>> test3 = new ArrayList<>();
//        Map<String, Object> map1 = new HashMap<>();
//        Map<String, Object> map2 = new HashMap<>();
//        map1.put("cc", "cc");
//        map1.put("dd", "dd");
//        map2.put("ee", "ee");
//        map2.put("ff", "uuuuuuuuu");
//        test3.add(map1);
//        test3.add(map2);
//        test.setTest3(test3);
//        MongodbClient.getInstance("mongodb://10.1.30.102:27017").updateDocAndListBySet("test", "test", "000022", test, Test.class, true);
//    }


    public void updateDoc(String dataBaseName, String collectionName, Bson var1, List<? extends Bson> list) {
        getCollectionByName(dataBaseName, collectionName).updateOne(var1, list);
    }

    public void updateDocs(String dataBaseName, String collectionName, Bson var1, Bson var2) {
        getCollectionByName(dataBaseName, collectionName).updateMany(var1, var2);
    }

    public void updateDocs(String dataBaseName, String collectionName, Bson var1, List<? extends Bson> list) {
        getCollectionByName(dataBaseName, collectionName).updateMany(var1, list);
    }


    public DeleteResult deleteDoc(String dataBaseName, String collectionName, Bson var1) {
        return getCollectionByName(dataBaseName, collectionName).deleteOne(var1);
    }

    public DeleteResult deleteDocs(String dataBaseName, String collectionName, Bson var1) {
        return getCollectionByName(dataBaseName, collectionName).deleteMany(var1);
    }

    public BulkWriteResult bulkWrite(String dataBaseName, String collectionName, List<? extends WriteModel<? extends Document>> listData) {
        return getCollectionByName(dataBaseName, collectionName).bulkWrite(listData);
    }

    public UpdateResult replaceDoc(String dataBaseName, String collectionName, Bson var1, Document var2) {
        return getCollectionByName(dataBaseName, collectionName).replaceOne(var1, var2);
    }

    public UpdateResult replaceDoc(String dataBaseName, String collectionName, Bson var1, Document var2, ReplaceOptions var3) {
        return getCollectionByName(dataBaseName, collectionName).replaceOne(var1, var2, var3);
    }

    public UpdateResult replaceDoc(String dataBaseName, String collectionName, Bson var1, Document var2, UpdateOptions var3) {
        return getCollectionByName(dataBaseName, collectionName).replaceOne(var1, var2, var3);
    }

    //查询总数
    public long countDocs(String dataBaseName, String collectionName, Bson var1) {
        if (null == var1) {
            return getCollectionByName(dataBaseName, collectionName).countDocuments();
        }
        return getCollectionByName(dataBaseName, collectionName).countDocuments(var1);
    }

    public FindIterable<Document> findMongoDbDoc(String dataBaseName, String collectionName, BasicDBObject basicDBObject, Integer skip, Integer limit) {
        return getCollectionByName(dataBaseName, collectionName).find(basicDBObject).skip(skip).limit(limit);
    }
}
