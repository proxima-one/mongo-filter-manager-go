package example_model

import (
	"github.com/proxima-one/mongo-filter-manager-go/driver"
	"time"
)

type Cat struct {
	Id        string    `json:"id" bson:"_id"`
	Name      string    `json:"name" bson:"name"`
	Breed     string    `json:"breed" bson:"breed"`
	BirthDate time.Time `json:"birthDate" bson:"birthDate"`
}

func (c *Cat) GetId() string {
	return c.Id
}

func (c *Cat) HaveId() bool {
	return c.Id != ""
}

func (c *Cat) CollectionName() string {
	return "cats"
}

func (c *Cat) Action() driver.StoreAction {
	return driver.StoreActionCreate
}

func (c *Cat) IsState() bool {
	return false
}

func (c *Cat) DefaultSortKeypath() string {
	return "birthDate"
}

func (c *Cat) SetAction(_ driver.StoreAction) {
}
