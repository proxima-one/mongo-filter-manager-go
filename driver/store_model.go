package driver

import "time"

const StoreActionDelete = "delete"
const StoreActionUpdate = "update"
const StoreActionCreate = "create"

type CanBeStored interface {
	HaveId() bool
	GetId() string
	CollectionName() string
	IsState() bool
	Action() string
	DefaultSortKeypath() string
}

type NewCanBeStoredFunc func() CanBeStored

type TimeFilter struct {
	From    *time.Time
	To      *time.Time
	Keypath string
}

type Paging struct {
	Limit  int
	Offset int
}

func (pf *TimeFilter) SetStart(start time.Time) *TimeFilter {
	pf.From = &start
	return pf
}

func (pf *TimeFilter) SetEnd(end time.Time) *TimeFilter {
	pf.To = &end
	return pf
}
