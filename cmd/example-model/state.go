package example_model

import "github.com/proxima-one/mongo-filter-manager-go/driver"

type State struct {
	Id        string `json:"id" bson:"_id"`
	LastCatID string `json:"lastCatId" bson:"lastCatId"`
}

func (s *State) GetId() string {
	return s.Id
}

func (s *State) HaveId() bool {
	return s.Id != ""
}

func (s *State) CollectionName() string {
	return "state"
}

func (s *State) IsState() bool {
	return true
}

func (s *State) Action() driver.StoreAction {
	return driver.StoreActionUpdate
}

func (s *State) DefaultSortKeypath() string {
	return ""
}

func (s *State) SetAction(_ driver.StoreAction) {
}
