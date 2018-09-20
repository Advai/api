package cache

import (
	"github.com/HackIllinois/api/common/config"
	"github.com/go-redis/redis"
)

// Cache interface exposing the methods necessary to querying, inserting, updating, upserting, and removing records
type Cache interface {
	Connect(host string) error
	Set(key string, value interface{}) (interface{}, error)
	Get(key string) (string, error)
	MultiSet(pairs ...interface{})
	MultiGet(keys ...string) ([]interface{}, error)
	Delete(keys ...string) (int, error)
}

// RedisCache struct which implements the Cache interface
type RedisCache struct {
	global_session *redis.Client
}

// Connect to cache server using host which is in ip:port format
func (cache *RedisCache) Connect(host string) error {
	cache.global_session = redis.NewClient(&redis.Options{
		Addr:     host,
		Password: config.CACHE_PASSWORD,
		DB:       0, // use default DB
	})

	_, err := cache.global_session.Ping().Result()

	return err
}

// Set key value with expiration date
func (cache *RedisCache) Set(key string, value interface{}) error {
	// setting expiration to 0, so it doesnt expire
	return cache.global_session.Set(key, value, 0).Err()
}

// Get key value and return in string format
func (cache *RedisCache) Get(key string) (string, error) {
	return cache.global_session.Get(key).Result()
}

// MultiSet sets multiple key value pairs
func (cache *RedisCache) MultiSet(pairs ...interface{}) error {
	return cache.global_session.MSet(pairs).Err()
}

// MultiGet gets multiple keys values
func (cache *RedisCache) MultiGet(keys ...string) ([]interface{}, error) {
	return cache.global_session.MGet(keys...).Result()
}

// Delete keys and their values and returns amount actually deleted
func (cache *RedisCache) Delete(keys ...string) (int64, error) {
	return cache.global_session.Del(keys...).Result()
}
