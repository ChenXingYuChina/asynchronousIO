package asynchronousIO

import (
	"sync"
)

type DataSource interface {
	Load(key Key) (Bean, error)
	Save(bean Bean) error
	Delete(key Key) error
}

type Bean interface {
	GetKey() Key
}

type AsynchronousIOMachine interface {
	Load(key Key, source uint16) func() (Bean, error)
	Save(bean Bean, source uint16)
	// must call the return function
	Delete(key Key, source uint16) func() error
	SaveAndCallBackWhenFinish(bean Bean, source uint16) func() error
}

type AsynchronousIOError struct {
	E error
}

func (e AsynchronousIOError) Error() string {
	return e.E.Error()
}

func (AsynchronousIOError) GetKey() Key {
	return nil
}

type Finish struct{}

func (Finish) GetKey() Key {
	return nil
}

type Key interface {
	UniqueId() (int64, bool)
	ToString() (string, bool)
	TypeId() int64
}

const UnknownType uint16 = 0xffff

type UnknownTypeError struct{}

func (UnknownTypeError) Error() string {
	return "unknown type"
}

type MemorySource interface {
	Alloc(length int) interface{}
	Free(interface{})
}

type CmdTable interface {
	Save(k int64, bean Bean, notify bool, cs *sync.Pool) (c chan Bean, start bool)
	Load(k int64, cs *sync.Pool) (c chan Bean, bean Bean)
	Delete(k int64, cs *sync.Pool) (c chan Bean, start bool)
	Get(k int64, lastCmd uint8, lastBean Bean) (cmd uint8, bean Bean, c chan Bean, restart bool)
}

type Start struct{}

func (Start) GetKey() Key {
	panic("implement me")
}
