package example_model

import (
	"github.com/proxima-one/mongo-filter-manager-go/driver"
	"time"
)

type Dog struct {
	Id        string    `json:"id" bson:"_id"`
	Name      string    `json:"name" bson:"name"`
	Breed     string    `json:"breed" bson:"breed"`
	BirthDate time.Time `json:"birthDate" bson:"birthDate"`
}

func (d *Dog) GetId() string {
	return d.Id
}

func (d *Dog) HaveId() bool {
	return d.Id != ""
}

func (d *Dog) CollectionName() string {
	return "dogs"
}

func (d *Dog) Action() driver.StoreAction {
	return driver.StoreActionCreate
}

func (d *Dog) IsState() bool {
	return false
}

func (d *Dog) DefaultSortKeypath() string {
	return "birthDate"
}

func (d *Dog) SetAction(_ driver.StoreAction) {
}
