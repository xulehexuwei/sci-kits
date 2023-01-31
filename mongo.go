package scikits

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

func MongoDbClient() *mongo.Database {
	label := "mongo"
	host := MyViper.GetString(fmt.Sprintf("%s.host", label))
	port := MyViper.GetString(fmt.Sprintf("%s.host", label))
	user := MyViper.GetString(fmt.Sprintf("%s.host", label))
	pw := MyViper.GetString(fmt.Sprintf("%s.host", label))
	db := MyViper.GetString(fmt.Sprintf("%s.host", label))

	// [mongodb://][user:pass@]host1[:port1][,host2[:port2],...][/database][?options]
	mongoURI := fmt.Sprintf("mongodb://%s:%s@%s:%v/%s", user, pw, host, port, db)
	clientOptions := options.Client().ApplyURI(mongoURI)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	mongoDB := client.Database(db)
	return mongoDB
}

func MongoUpdate(MongoDB *mongo.Database, colName string, filter bson.M, update bson.M) (*mongo.UpdateResult, error) {
	collection := MongoDB.Collection(colName)
	updateBson := bson.M{"$set": update}
	res, err := collection.UpdateOne(context.TODO(), filter, updateBson)
	return res, err
}

//func MongoBulkUpdate(MongoDB *mongo.Database, colName string, arr ) (*mongo.UpdateResult, error) {
//	collection := MongoDB.Collection(colName)
//	res, err := collection.BulkWrite(arr)
//	return res, err
//}

// 根据唯一键有则更新无则添加
func MongoUpdateOrInsert(MongoDB *mongo.Database, colName string, filter bson.M, bMap bson.M) error {
	timeNow := time.Now().Unix()
	bMap["CreateTime"] = timeNow
	_, err := MongoInsertOne(MongoDB, colName, bMap)
	if err != nil {
		delete(bMap, "CreateTime")
		bMap["UpdateTime"] = timeNow
		err = MongoFindOneAndUpdate(MongoDB, colName, filter, bMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func MongoFindOneAndUpdate(MongoDB *mongo.Database, colName string, filter bson.M, update bson.M) error {
	updateBson := bson.M{"$set": update}
	collection := MongoDB.Collection(colName)
	res := collection.FindOneAndUpdate(context.TODO(), filter, updateBson)
	err := res.Err()
	if err != nil {
		//log.Fatal(err)
		return err
	} else {
		return nil
	}
}

func MongoInsertOne(MongoDB *mongo.Database, colName string, document interface{}) (*mongo.InsertOneResult, error) {
	collection := MongoDB.Collection(colName)
	res, err := collection.InsertOne(context.TODO(), document)
	return res, err
}

func MongoJudgeExist(MongoDB *mongo.Database, colName string, filter bson.M) bool {
	collection := MongoDB.Collection(colName)
	singleResult := collection.FindOne(context.TODO(), filter)
	err := singleResult.Err()
	if err != nil {
		return false
	} else {
		return true
	}
}

func MongoFindOneLoadStruct(MongoDB *mongo.Database, colName string, filter bson.M, model interface{}) error {
	collection := MongoDB.Collection(colName)
	singleResult := collection.FindOne(context.TODO(), filter)
	err := singleResult.Decode(model)
	return err
}

func MongoFindAll(MongoDB *mongo.Database, colName string, filter bson.M, opts ...*options.FindOptions) []map[string]interface{} {
	collection := MongoDB.Collection(colName)
	cur, _ := collection.Find(context.TODO(), filter, opts...)
	defer cur.Close(context.TODO())
	results := getMongoListDataByCur(cur)
	return results
}

func getMongoListStructByCur(cur *mongo.Cursor, responseStruct struct{}) []struct{} {
	var results []struct{}
	for cur.Next(context.TODO()) {
		// 创建一个值，将单个文档解码为该值
		tmpStruct := responseStruct
		err := cur.Decode(&tmpStruct)
		if err != nil {
			log.Fatal(err)
		}
		results = append(results, tmpStruct)
	}
	return results
}

func GetMongoFindCur(MongoDB *mongo.Database, colName string, filter bson.M, opts ...*options.FindOptions) *mongo.Cursor {
	collection := MongoDB.Collection(colName)
	cur, _ := collection.Find(context.TODO(), filter, opts...)
	return cur
}

func MongoCount(MongoDB *mongo.Database, colName string, filter bson.M) int64 {
	collection := MongoDB.Collection(colName)
	num, _ := collection.CountDocuments(context.TODO(), filter, nil)
	return num
}

func getMongoListDataByCur(cur *mongo.Cursor) []map[string]interface{} {
	var results []map[string]interface{}
	for cur.Next(context.TODO()) {
		// 创建一个值，将单个文档解码为该值
		var elem map[string]interface{}
		err := cur.Decode(&elem)
		if err != nil {
			log.Fatal(err)
		}
		results = append(results, elem)
	}
	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}
	return results
}

func MongoSql(MongoDB *mongo.Database, colName string, filter bson.M, opts *options.FindOptions) []map[string]interface{} {
	collection := MongoDB.Collection(colName)
	cur, err := collection.Find(context.TODO(), filter, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer cur.Close(context.TODO())
	results := getMongoListDataByCur(cur)
	return results
}
