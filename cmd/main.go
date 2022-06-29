package main

import (
	"fmt"
	model "github.com/proxima-one/mongo-filter-manager-go/cmd/example-model"
	"github.com/proxima-one/mongo-filter-manager-go/driver"
	"time"
)

func PanicOnErr(err error) {
	panic(err)
}

type CatFilter struct {
	Breed string `json:"breed" bson:"breed"`
}

func cat1() model.Cat {
	return model.Cat{
		Id:        "1",
		Name:      "Big Cat",
		Breed:     "iconic",
		BirthDate: time.Now(),
	}
}

func cat2() model.Cat {
	return model.Cat{
		Id:        "2",
		Name:      "Small Cat",
		Breed:     "not iconic",
		BirthDate: time.Now().Add(time.Hour),
	}
}

func SaveAndRead(repo *driver.MongoRepo) {
	cat1 := cat1()
	cat2 := cat2()
	PanicOnErr(repo.SaveObject(nil, &cat1))
	PanicOnErr(repo.SaveObject(nil, &cat2))

	ct1, err := repo.ReadObject(nil, "1", func() driver.CanBeStored {
		return &model.Cat{}
	})
	if err != nil {
		panic(err)
	}
	if ct1.(*model.Cat).Name != cat1.Name {
		panic("cat1 not saved")
	}
	ct2, err := repo.ReadObject(nil, "2", func() driver.CanBeStored {
		return &model.Cat{}
	})
	if err != nil {
		panic(err)
	}
	if ct2.(*model.Cat).Name != cat2.Name {
		panic("cat2 not saved")
	}
}

func ReadFiltered(repo *driver.MongoRepo) {
	// Create a new filter
	catFilter := CatFilter{Breed: "iconic"}
	cats, err := repo.ReadObjectsWithObjectFilter(
		nil,
		catFilter,
		nil,
		nil,
		func() driver.CanBeStored {
			return &model.Cat{}
		})
	if err != nil {
		panic(err)
	}
	for _, cat := range cats {
		fmt.Println(cat.(*model.Cat).Name)
	}
}

func main() {
	dog1 := &model.Dog{
		Id:    "1",
		Name:  "Big Dog",
		Breed: "iconic",
	}
	state := &model.State{
		Id:        "example",
		LastCatID: "0",
	}
	cat := cat1()
	repo := driver.NewMongoRepo(
		"mongodb://localhost:27017",
		"test",
		[]string{cat.CollectionName(), dog1.CollectionName()},
		state.CollectionName(),
		func(err error) {
			fmt.Println(err)
		})
	err := repo.Connect()
	if err != nil {
		panic(err)
	}
	SaveAndRead(repo)

	ReadFiltered(repo)

	PanicOnErr(repo.Drop())

}
