package com.yongqing.mongodb.client;

import com.mongodb.BasicDBObject;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.Map;

/**
 *
 */
public interface Client {

    void intiClientConnection();

    void close();

    void createCollection(String dataBaseName, String collectionName);

    MongoDatabase getDatabase(String dataBaseName);

    List<String> listCollectionNames(String dataBaseName);

    MongoCollection<Document> getCollectionByName(String dataBaseName, String collectionName);

    FindIterable<Document> findMongoDbDocById(String dataBaseName, String collectionName, String id);

    FindIterable<Document> findMongoDbDocByIdRegex(String dataBaseName, String collectionName, String id);

    FindIterable<Document> findMongoDbDocById(String dataBaseName, String collectionName, String startId, String endId);

    FindIterable<Document> findMongoDbDoc(String dataBaseName, String collectionName, BasicDBObject basicDBObject);

    FindIterable<Document> findMongoDbDoc(String dataBaseName, String collectionName, BasicDBObject basicDBObject, Integer limitNum);

    FindIterable<Document> findMongoDbDocById(String dataBaseName, String collectionName, String startId, String endId, Integer limitNum);

    FindIterable<Document> findMongoDbDocByIdDescSort(String dataBaseName, String collectionName, String startId, String endId, String sortField);

    FindIterable<Document> findMongoDbDocByIdDescSort(String dataBaseName, String collectionName, String startId, String endId, String sortField, Integer limitNum);

    FindIterable<Document> findMongoDbDocByIdAscSort(String dataBaseName, String collectionName, String startId, String endId, String sortField);

    FindIterable<Document> findMongoDbDocByIdAscSort(String dataBaseName, String collectionName, String startId, String endId, String sortField, Integer limitNum);

    void insertDoc(String dataBaseName, String collectionName, Document document);

    void insertDoc(String dataBaseName, String collectionName, List<? extends Document> listData);

    void updateDoc(String dataBaseName, String collectionName, Bson var1, Bson var2);

    void updateDoc(String dataBaseName, String collectionName, Bson var1, List<? extends Bson> list);

    void updateDocs(String dataBaseName, String collectionName, Bson var1, Bson var2);

    void updateDocs(String dataBaseName, String collectionName, Bson var1, List<? extends Bson> list);

    DeleteResult deleteDoc(String dataBaseName, String collectionName, Bson var1);

    DeleteResult deleteDocs(String dataBaseName, String collectionName, Bson var1);

    BulkWriteResult bulkWrite(String dataBaseName, String collectionName, List<? extends WriteModel<? extends Document>> listData);

    UpdateResult replaceDoc(String dataBaseName, String collectionName, Bson var1, Document var2);

    FindIterable<Document> findMongoDbDocByField(String dataBaseName, String collectionName, String field, String fieldValue, Integer limitNum);

    FindIterable<Document> findMongoDbDocByField(String dataBaseName, String collectionName, String field, String fieldValue);

    FindIterable<Document> findMongoDbDocByFieldRegex(String dataBaseName, String collectionName, String field, String fieldValue, Integer limitNum);

    FindIterable<Document> findMongoDbDocByFieldRegex(String dataBaseName, String collectionName, String field, String fieldValue);

    FindIterable<Document> findMongoDbDocByField(String dataBaseName, String collectionName, String field, String fieldValue, Integer limitNum, String sortField);

    FindIterable<Document> findMongoDbDocByFieldRegex(String dataBaseName, String collectionName, String field, String fieldValue, Integer limitNum, String sortField);

    FindIterable<Document> findMongoDbDocByField(String dataBaseName, String collectionName, String field, String fieldValue, String sortField);

    FindIterable<Document> findMongoDbDocByFieldRegex(String dataBaseName, String collectionName, String field, String fieldValue, String sortField);

    FindIterable<Document> findMongoDbDocByField(String dataBaseName, String collectionName, Map<String, String> fields, Integer limitNum, String sortField);

    FindIterable<Document> findMongoDbDocByField(String dataBaseName, String collectionName, Map<String, String> fields, Integer limitNum);

    FindIterable<Document> findMongoDbDocByFieldRegex(String dataBaseName, String collectionName, Map<String, String> fields, Integer limitNum);

    FindIterable<Document> findMongoDbDocByField(String dataBaseName, String collectionName, Map<String, String> fields);

    FindIterable<Document> findMongoDbDocByField(String dataBaseName, String collectionName, Map<String, String> fields, String sortField);

    FindIterable<Document> findMongoDbDocByFieldRegex(String dataBaseName, String collectionName, Map<String, String> fields, Integer limitNum, String sortField);

    FindIterable<Document> findMongoDbDocByFieldRegex(String dataBaseName, String collectionName, Map<String, String> fields);

    FindIterable<Document> findMongoDbDocByFieldRegex(String dataBaseName, String collectionName, Map<String, String> fields, String sortField);

    public <T> void updateDocAndListByPush(String dataBaseName, String collectionName, Bson var1, T data, Class<T> tclass, Boolean upsert) throws IllegalAccessException;

    public <T> void updateDocAndListBySet(String dataBaseName, String collectionName, Bson var1, T data, Class<T> tclass, Boolean upsert) throws IllegalAccessException;

    public <T> void updateDocAndListBySet(String dataBaseName, String collectionName, T data, Class<T> tclass, Boolean upsert) throws IllegalAccessException;

    public <T> void updateDocAndListByPush(String dataBaseName, String collectionName, T data, Class<T> tclass, Boolean upsert) throws IllegalAccessException;

    public <T> void updateDocAndListBySet(String dataBaseName, String collectionName, String searchDocId, T data, Class<T> tclass, Boolean upsert) throws IllegalAccessException;

    public <T> void updateDocAndListByPush(String dataBaseName, String collectionName, String searchDocId, T data, Class<T> tclass, Boolean upsert) throws IllegalAccessException;

}
