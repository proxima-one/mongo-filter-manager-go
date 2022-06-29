package driver

import (
	"context"
	"errors"
	"fmt"
	"github.com/proxima-one/mongo-index-manager-go/index_manager"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
	"sync"
	"time"
)

const PrimaryKeyKeypath = "_id"
const defaultTimeout = 30 * time.Second
const defaultDBName = "test"
const defaultCacheSize = 1000
const defaultCacheTimeout = time.Second

var ErrNoId = errors.New("no Id is set to object")

type FilterTuple struct {
	FilterObject interface{}
	Prototype    CanBeStored
}

func BsonTimeFilter(pf *TimeFilter) bson.D {
	if pf.From != nil && pf.To != nil {
		return bson.D{{pf.Keypath, bson.M{"$gte": pf.From, "$lte": pf.To}}}
	} else if pf.From != nil {
		return bson.D{{pf.Keypath, bson.D{{"$gte", pf.From}}}}
	} else if pf.To != nil {
		return bson.D{{pf.Keypath, bson.D{{"$lte", pf.To}}}}
	}
	return bson.D{}
}

func AndBsonFilter(filters []bson.D) bson.D {
	resFilters := make([]bson.D, 0)
	for _, filter := range filters {
		if len(filter) > 0 {
			resFilters = append(resFilters, filter)
		}
	}
	if len(resFilters) == 0 {
		return bson.D{}
	}
	if len(resFilters) == 1 {
		return filters[0]
	}
	return bson.D{{"$and", filters}}
}

func GenericIdsFilterBson(ids []string) bson.D {
	if ids != nil || len(ids) > 0 {
		return bson.D{{PrimaryKeyKeypath, bson.M{"$in": ids}}}
	}
	return bson.D{}
}

func ModelFilterToBsonFilter(filter interface{}) bson.D {
	var filters []bson.D
	typeRefl := reflect.TypeOf(filter)
	valueRefl := reflect.ValueOf(filter)
	for i := 0; i < typeRefl.NumField(); i++ {
		value := valueRefl.Field(i)
		fieldKind := value.Kind()
		if fieldKind != reflect.Pointer {
			panic(fmt.Sprintf("field is not pointer. Not implemented for other types"))
			continue
		}
		valKind := value.Elem().Kind()
		if valKind != reflect.String {
			panic(fmt.Sprintf("field is not *string. Filter support only *string for now"))
			continue
		}
		if !value.IsNil() {
			name := typeRefl.Field(i).Tag.Get("bson")
			filters = append(filters, bson.D{{name, *value.Interface().(*string)}})
		}
	}
	return AndBsonFilter(filters)
}

type MongoMetrics struct {
	TimePer1000savings time.Duration
}

type MongoRepo struct {
	client          *mongo.Client
	uri             string
	dbName          string
	processingQueue *ObjectQueue
	metrics         *MongoMetrics
}

func NewMongoRepo(uri string, db string, supportedCollectionNames []string, stateCollectionName string, errCallback func(err error)) *MongoRepo {
	if db == "" {
		db = defaultDBName
	}
	result := &MongoRepo{uri: uri, dbName: db, metrics: &MongoMetrics{}}
	allCollections := append(supportedCollectionNames, stateCollectionName)
	processingQueue := NewQueue(allCollections, defaultCacheSize, defaultCacheTimeout)
	processingQueue.StartProcessing(context.Background(), func(cache QueueCache) {
		start := time.Now()
		cnt := 0
		wg := sync.WaitGroup{}
		wg.Add(0)
		for _, collectionName := range supportedCollectionNames {
			key := collectionName
			val := cache[key]
			if val == nil || len(val) == 0 {
				continue
			}
			documentsToInsert := make([]interface{}, 0, len(val))
			documentsToUpdate := make([]mongo.WriteModel, 0, len(val))
			documentsToDelete := make([]interface{}, 0, len(val))

			for _, obj := range val {
				if obj.Action() == StoreActionCreate {
					documentsToInsert = append(documentsToInsert, obj)
				}
				if obj.Action() == StoreActionUpdate {
					documentsToUpdate = append(documentsToUpdate,
						mongo.NewReplaceOneModel().SetFilter(bson.D{{PrimaryKeyKeypath, obj.GetId()}}).SetReplacement(obj).SetUpsert(true))
				}
				if obj.Action() == StoreActionDelete {
					documentsToDelete = append(documentsToDelete, obj)
				}
			}
			if len(documentsToInsert) > 0 {
				wg.Add(1)
				cnt += len(documentsToInsert)
				go func() {
					_, err := result.collection(key).InsertMany(context.Background(), documentsToInsert, options.InsertMany())
					if err != nil {
						errCallback(err)
					}
					wg.Done()
				}()
			}
			if len(documentsToUpdate) > 0 {
				wg.Add(1)
				cnt += len(documentsToUpdate)
				go func() {
					_, err := result.collection(key).BulkWrite(context.Background(), documentsToUpdate, options.BulkWrite())
					if err != nil {
						errCallback(err)
					}
					wg.Done()
				}()
			}
			if len(documentsToDelete) > 0 {
				wg.Add(1)
				cnt += len(documentsToDelete)
				go func() {
					_, err := result.collection(key).DeleteMany(context.Background(), bson.M{PrimaryKeyKeypath: bson.M{"$in": documentsToDelete}})
					if err != nil {
						errCallback(err)
					}
					wg.Done()
				}()
			}
		}
		wg.Wait()

		key := stateCollectionName
		val := cache[key]
		if val == nil || len(val) == 0 {
			return
		}
		if len(val) > 1 {
			errCallback(fmt.Errorf("more than one state object in the queue"))
			return
		}
		for _, state := range val {
			err := result.UpdateObject(context.Background(), state)
			if err != nil {
				errCallback(err)
			}
		}
		result.metrics.TimePer1000savings = time.Duration(float64(time.Since(start)) * 1000 / float64(cnt))
	})
	result.processingQueue = processingQueue
	return result
}

func defaultContext() context.Context {
	ctx := context.Background()
	return ctx
}

func (repo *MongoRepo) GetDBName() string {
	return repo.dbName
}

func (repo *MongoRepo) Connect() error {
	client, err := mongo.NewClient(options.Client().ApplyURI(repo.uri))
	if err != nil {
		return err
	}
	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)
	err = client.Connect(ctx)
	if err != nil {
		return err
	}
	repo.client = client
	return nil
}

func (repo *MongoRepo) Disconnect() error {
	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)
	return repo.client.Disconnect(ctx)
}

func (repo *MongoRepo) collection(name string) *mongo.Collection {
	db := repo.client.Database(repo.dbName)
	if db == nil {
		return nil
	}
	return repo.client.Database(repo.dbName).Collection(name)
}

func (repo *MongoRepo) objectsFromCollectionByIdsAndTimeFrameWithLimit(
	ctx context.Context,
	collectionName string,
	ids []string,
	paging *Paging) (*mongo.Cursor, error) {
	return repo.objectsByUserFilterAndPageFilter(
		ctx,
		collectionName,
		GenericIdsFilterBson(ids),
		nil,
		"",
		paging)
}

func (repo *MongoRepo) objectFromCollectionById(ctx context.Context,
	collectionName string,
	id string) (*mongo.SingleResult, error) {

	res := repo.collection(collectionName).FindOne(ctx, bson.M{PrimaryKeyKeypath: id})
	if res.Err() != nil {
		if res.Err() == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, res.Err()
	}
	return res, nil
}

func (repo *MongoRepo) Drop() error {
	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)
	return repo.client.Database(repo.dbName).Drop(ctx)
}

func (repo *MongoRepo) UpdateObject(ctx context.Context, obj CanBeStored) error {
	if ctx == nil {
		ctx = defaultContext()
	}
	if !obj.HaveId() {
		return ErrNoId
	}
	id := obj.GetId()
	_, err := repo.collection(obj.CollectionName()).UpdateOne(ctx, bson.M{PrimaryKeyKeypath: id}, bson.M{"$set": obj})
	if err != nil {
		return err
	}
	return nil
}

func (repo *MongoRepo) SaveObject(ctx context.Context, obj CanBeStored) error {
	if ctx == nil {
		ctx = defaultContext()
	}
	if !obj.HaveId() {
		return ErrNoId
	}
	id := obj.GetId()
	res, err := repo.objectFromCollectionById(ctx, obj.CollectionName(), id)
	if err != nil {
		return err
	}
	if res == nil {
		_, err := repo.collection(obj.CollectionName()).InsertOne(ctx, obj)
		if err != nil {
			return err
		}
	} else {
		_, err := repo.collection(obj.CollectionName()).UpdateOne(ctx, bson.M{PrimaryKeyKeypath: id}, bson.M{"$set": obj})
		if err != nil {
			return err
		}
	}
	return nil
}

func (repo *MongoRepo) InsertObject(ctx context.Context, obj CanBeStored) error {
	if ctx == nil {
		ctx = defaultContext()
	}
	if !obj.HaveId() {
		return ErrNoId
	}
	_, err := repo.collection(obj.CollectionName()).InsertOne(ctx, obj)
	if err != nil {
		return err
	}
	return nil
}

func (repo *MongoRepo) objectsByUserFilterAndPageFilter(
	ctx context.Context,
	collectionName string,
	userFilter bson.D,
	timeFilter *TimeFilter,
	sorting string,
	paging *Paging) (*mongo.Cursor, error) {

	collection := repo.collection(collectionName)
	var filters []bson.D
	opt := options.Find()
	if userFilter != nil && len(userFilter) > 0 {
		filters = append(filters, userFilter)
	}
	if timeFilter != nil && len(timeFilter.Keypath) > 0 {
		pageFilter := BsonTimeFilter(timeFilter)
		if len(pageFilter) > 0 {
			filters = append(filters, pageFilter)
		}
	}
	if paging != nil {
		if paging.Limit > 0 {
			opt.SetLimit(int64(paging.Limit))
		}
		opt.SetSkip(int64(paging.Offset))
	}
	if sorting != "" {
		opt.SetSort(bson.D{{sorting, 1}})
	}
	return collection.Find(ctx, AndBsonFilter(filters), opt)
}

func (repo *MongoRepo) DeleteObject(ctx context.Context, obj CanBeStored) error {
	if ctx == nil {
		ctx = defaultContext()
	}
	if !obj.HaveId() {
		return ErrNoId
	}
	err := repo.collection(obj.CollectionName()).FindOneAndDelete(ctx, bson.M{PrimaryKeyKeypath: obj.GetId()}).Err()
	if err != nil {
		return err
	}
	return nil
}

func (repo *MongoRepo) ReadObject(ctx context.Context, id string, objFactory NewCanBeStoredFunc) (CanBeStored, error) {
	if ctx == nil {
		ctx = defaultContext()
	}
	result := objFactory()
	res, err := repo.objectFromCollectionById(ctx, result.CollectionName(), id)
	if err != nil {
		return nil, err
	}
	if res != nil {
		errDecode := res.Decode(result)
		if errDecode != nil {
			return nil, errDecode
		}
		return result, nil
	} else {
		return nil, nil
	}
}

func decodeStored(ctx context.Context, objFactory NewCanBeStoredFunc, cursor *mongo.Cursor) ([]CanBeStored, error) {
	var result []CanBeStored
	for cursor.Next(ctx) {
		obj := objFactory()
		err := cursor.Decode(obj)
		if err != nil {
			return nil, err
		}
		result = append(result, obj)
	}
	return result, nil
}

func (repo *MongoRepo) ReadObjectsWithObjectFilter(
	ctx context.Context,
	filterObj interface{},
	timeFilter *TimeFilter,
	paging *Paging,
	objFactory NewCanBeStoredFunc) ([]CanBeStored, error) {

	if ctx == nil {
		ctx = defaultContext()
	}
	filter := ModelFilterToBsonFilter(filterObj)
	prototype := objFactory()
	cursor, err := repo.objectsByUserFilterAndPageFilter(
		ctx,
		objFactory().CollectionName(),
		filter,
		timeFilter,
		prototype.DefaultSortKeypath(),
		paging)

	if err != nil {
		return nil, err
	}
	return decodeStored(ctx, objFactory, cursor)
}

func (repo *MongoRepo) ReadObjectsWithFilter(
	ctx context.Context,
	filter bson.D,
	timeFilter *TimeFilter,
	paging *Paging,
	objFactory NewCanBeStoredFunc) ([]CanBeStored, error) {

	if ctx == nil {
		ctx = defaultContext()
	}
	prototype := objFactory()
	cursor, err := repo.objectsByUserFilterAndPageFilter(ctx,
		objFactory().CollectionName(),
		filter,
		timeFilter,
		prototype.DefaultSortKeypath(),
		paging)
	if err != nil {
		return nil, err
	}
	return decodeStored(ctx, objFactory, cursor)
}

func (repo *MongoRepo) ReadObjectsWithIds(
	ctx context.Context,
	ids []string,
	objFactory NewCanBeStoredFunc) ([]CanBeStored, error) {

	if ctx == nil {
		ctx = defaultContext()
	}
	filter := GenericIdsFilterBson(ids)
	cursor, err := repo.objectsByUserFilterAndPageFilter(ctx,
		objFactory().CollectionName(),
		filter,
		nil,
		"",
		nil)
	if err != nil {
		return nil, err
	}
	return decodeStored(ctx, objFactory, cursor)
}

func (repo *MongoRepo) PushObjectToSaveQueue(data CanBeStored) {
	repo.processingQueue.Push(data)
}

func (repo *MongoRepo) SaveQueue() error {
	repo.processingQueue.Save()
	return nil
}

func (repo *MongoRepo) Metrics() MongoMetrics {
	return *repo.metrics
}

func (repo *MongoRepo) SyncIndexes(ctx context.Context, indexes map[string][]bson.D) error {
	if ctx == nil {
		ctx = defaultContext()
	}
	//todo: can be done concurrently with ctx canceling
	for collectionName, idxes := range indexes {
		err := index_manager.SyncIndexes(
			ctx,
			repo.collection(collectionName),
			idxes)
		if err != nil {
			return err
		}
	}
	return nil
}

func indexesNamesFromFilterObject(filter interface{}) []bson.D {
	var result []bson.D
	if filter == nil {
		return nil
	}
	typeRefl := reflect.TypeOf(filter)
	valueRefl := reflect.ValueOf(filter)
	for i := 0; i < typeRefl.NumField(); i++ {
		field := typeRefl.Field(i)
		fieldKind := valueRefl.Field(i).Kind()
		if fieldKind != reflect.Pointer {
			panic(fmt.Sprintf("field %s is not pointer. Not implemented for other types", field.Name))
			continue
		}
		valKind := valueRefl.Field(i).Elem().Kind()
		if valKind != reflect.String {
			panic(fmt.Sprintf("field %s is not string. Filter support only *string for now", field.Name))
			continue
		}
		name := typeRefl.Field(i).Tag.Get("bson")
		result = append(result, bson.D{{name, int32(1)}})
	}
	return result
}

func MongoFiltersForFiltersObjects(filters []FilterTuple) map[string][]bson.D {
	result := make(map[string][]bson.D)
	for _, filter := range filters {
		item := make(map[string][]bson.D)
		bsonsForFilter := indexesNamesFromFilterObject(filter.FilterObject)
		if filter.Prototype.DefaultSortKeypath() != "" {
			bsonsForFilter = append(bsonsForFilter, bson.D{{filter.Prototype.DefaultSortKeypath(), int32(1)}})
		}
		item[filter.Prototype.CollectionName()] = bsonsForFilter
		result = MergeFilters(result, item)
	}
	return result
}

func MergeFilters(requiredIndexes, customIdxs map[string][]bson.D) map[string][]bson.D {
	for collectionName, customIndexes := range customIdxs {
		if _, ok := requiredIndexes[collectionName]; !ok {
			requiredIndexes[collectionName] = customIndexes
		} else {
			requiredIndexes[collectionName] = append(requiredIndexes[collectionName], customIndexes...)
		}
	}
	return requiredIndexes
}
