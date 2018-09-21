package database

import (
	"crypto/tls"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/mgo.v2/bson"
	"net"
	"strings"
	"time"

	"github.com/HackIllinois/api/common/cache"
	"github.com/HackIllinois/api/common/config"
	//"github.com/mitchellh/mapstructure"
	"gopkg.in/mgo.v2"
)

var rcache *cache.RedisCache

/*
	Database interface exposing the methods necessary to querying, inserting, updating, upserting, and removing records
*/
type Database interface {
	Connect(host string) error
	FindOne(collection_name string, query interface{}, result interface{}) error
	FindAll(collection_name string, query interface{}, result interface{}) error
	RemoveOne(collection_name string, query interface{}) error
	RemoveAll(collection_name string, query interface{}) (*mgo.ChangeInfo, error)
	Insert(collection_name string, item interface{}) error
	Upsert(collection_name string, selector interface{}, update interface{}) (*mgo.ChangeInfo, error)
	Update(collection_name string, selector interface{}, update interface{}) error
	UpdateAll(collection_name string, selector interface{}, update interface{}) (*mgo.ChangeInfo, error)
}

/*
	MongoDatabase struct which implements the Database interface for a mongo database
*/
type MongoDatabase struct {
	global_session *mgo.Session
	name           string
}

type ReturnStruct struct {
	result interface{}
	err    error
}

/*
	Initialize connection to mongo database
*/
func InitMongoDatabase(host string, db_name string) (MongoDatabase, error) {
	dial_info, err := mgo.ParseURL(host)

	if err != nil {
		return MongoDatabase{}, err
	}

	if config.IS_PRODUCTION {
		dial_info.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			tls_config := &tls.Config{}
			connection, err := tls.Dial("tcp", addr.String(), tls_config)
			return connection, err
		}
		dial_info.Timeout = 60 * time.Second
	}

	session, err := mgo.DialWithInfo(dial_info)

	//create cache and connect
	rcache = new(cache.RedisCache)
	rcache.Connect(config.CACHE_HOST)

	db := MongoDatabase{
		global_session: session,
		name:           db_name,
	}

	return db, err
}

/*
	Returns a copy of the global session for use by a connection
*/
func (db MongoDatabase) GetSession() *mgo.Session {
	return db.global_session.Copy()
}

/*
	Find one element matching the given query parameters
	if err == nil {
				err := bson.UnmarshalJSON([]byte(json_result), result)
			}
*/
func (db MongoDatabase) FindOne(collection_name string, query interface{}, result interface{}) error {
	//make buffered channel for the two values, so they wont block
	result_chan := make(chan ReturnStruct, 2)
	//anonymous goroutine to get cache data
	go func(query bson.M, collection_name string, result_chan chan ReturnStruct) {
		if val, ok := query["id"]; ok {
			key := strings.Join([]string{collection_name, val.(string)}, ":")
			json_result, err := rcache.Get(key)
			result_chan <- ReturnStruct{json_result, err}
		}
	}(query.(bson.M), collection_name, result_chan)

	go func(query bson.M, collection_name string, result_chan chan ReturnStruct) {
		current_session := db.GetSession()
		defer current_session.Close()
		collection := current_session.DB(db.name).C(collection_name)

		var query_result interface{}
		err := collection.Find(query).One(&query_result)
		result_chan <- ReturnStruct{query_result, err}
	}(query.(bson.M), collection_name, result_chan)

	//block till we get first value
	first_result := <-result_chan
	if first_result.result != nil && first_result.result != "" && first_result.err == nil {
		if json, ok := first_result.result.(string); ok {
			err := bson.UnmarshalJSON([]byte(json), result)
			if err == nil {
				return err
			}
		} else {
			err := mapstructure.Decode(first_result.result, result)
			if err == nil {
				return err
			}
		}
	}
	//block till second value
	second_result := <-result_chan
	if json, ok := second_result.result.(string); ok {
		err := bson.UnmarshalJSON([]byte(json), result)
		return err
	} else {
		err := mapstructure.Decode(second_result.result, result)
		return err
	}
	return second_result.err
}

/*
	Find all elements matching the given query parameters
*/
func (db MongoDatabase) FindAll(collection_name string, query interface{}, result interface{}) error {
	current_session := db.GetSession()
	defer current_session.Close()

	collection := current_session.DB(db.name).C(collection_name)

	err := collection.Find(query).All(result)

	return err
}

/*
	Remove one element matching the given query parameters
*/
func (db MongoDatabase) RemoveOne(collection_name string, query interface{}) error {
	current_session := db.GetSession()
	defer current_session.Close()

	collection := current_session.DB(db.name).C(collection_name)

	err := collection.Remove(query)

	return err
}

/*
	Remove all elements matching the given query parameters
*/
func (db MongoDatabase) RemoveAll(collection_name string, query interface{}) (*mgo.ChangeInfo, error) {
	current_session := db.GetSession()
	defer current_session.Close()

	collection := current_session.DB(db.name).C(collection_name)

	change_info, err := collection.RemoveAll(query)

	return change_info, err
}

/*
	Insert the given item into the collection
*/
func (db MongoDatabase) Insert(collection_name string, item interface{}) error {
	current_session := db.GetSession()
	defer current_session.Close()

	collection := current_session.DB(db.name).C(collection_name)

	err := collection.Insert(item)

	return err
}

/*
	Upsert the given item into the collection i.e.,
	if the item exists, it is updated with the given values, else a new item with those values is created.
*/
func (db MongoDatabase) Upsert(collection_name string, selector interface{}, update interface{}) (*mgo.ChangeInfo, error) {
	current_session := db.GetSession()
	defer current_session.Close()

	collection := current_session.DB(db.name).C(collection_name)

	change_info, err := collection.Upsert(selector, update)

	return change_info, err
}

/*
	Finds an item based on the given selector and updates it with the data in update
*/
func (db MongoDatabase) Update(collection_name string, selector interface{}, update interface{}) error {
	current_session := db.GetSession()
	defer current_session.Close()

	collection := current_session.DB(db.name).C(collection_name)

	err := collection.Update(selector, update)

	return err
}

/*
	Finds all items based on the given selector and updates them with the data in update
*/
func (db MongoDatabase) UpdateAll(collection_name string, selector interface{}, update interface{}) (*mgo.ChangeInfo, error) {
	current_session := db.GetSession()
	defer current_session.Close()

	collection := current_session.DB(db.name).C(collection_name)

	change_info, err := collection.UpdateAll(selector, update)

	return change_info, err
}
