package M2N

import (
	"github.com/ChenXingyuChina/asynchronousIO"
	"sync"
)

type reIn struct {
	channelBuffer *sync.Pool
	dataSource    []asynchronousIO.DataSource
}

func (m *reIn) Load(key asynchronousIO.Key, source uint16) func() (asynchronousIO.Bean, error) {
	c := m.channelBuffer.Get().(chan asynchronousIO.Bean)
	go m.readWorker(key, source, c)
	return genResult(c, m.channelBuffer).get
}

func (m *reIn) Save(bean asynchronousIO.Bean, source uint16) {
	go m.writeWorker(bean, source, nil)
}

func (m *reIn) Delete(key asynchronousIO.Key, source uint16) func() error {
	c := m.channelBuffer.Get().(chan asynchronousIO.Bean)
	go m.deleteWorker(key, source, c)
	return genResult(c, m.channelBuffer).error
}

func (m *reIn) SaveAndCallBackWhenFinish(bean asynchronousIO.Bean, source uint16) func() error {
	c := m.channelBuffer.Get().(chan asynchronousIO.Bean)
	go m.writeWorker(bean, source, c)
	return genResult(c, m.channelBuffer).error
}

//func (m *reIn) Register(models []asynchronousIO.Bean, check func(key asynchronousIO.Key, source uint16) uint16) {
//	length := uint16(len(m.models))
//	m.models = append(m.models, models...)
//	m.check = func(oldLength uint16, oldCheck, newCheck func(key asynchronousIO.Key, source uint16) uint16) func(key asynchronousIO.Key, source uint16) uint16 {
//		return func(key asynchronousIO.Key, source uint16) uint16 {
//			t := oldCheck(key, source)
//			if t == asynchronousIO.UnknownType {
//				return oldLength + newCheck(key, source)
//			}
//			return t
//		}
//	}(length, m.check, check)
//}

func newReIn(source []asynchronousIO.DataSource) asynchronousIO.AsynchronousIOMachine {
	goal := &reIn{channelBuffer: new(sync.Pool), dataSource: source}
	goal.channelBuffer.New = func() interface{} {
		return make(chan asynchronousIO.Bean, 1)
	}
	return goal
}

func (m *reIn) readWorker(key asynchronousIO.Key, source uint16, out chan asynchronousIO.Bean) {
	goal, err := m.dataSource[source].Load(key)
	if err != nil {
		out <- asynchronousIO.AsynchronousIOError{E: asynchronousIO.UnknownTypeError{}}
	}
	out <- goal
}

func (m *reIn) writeWorker(bean asynchronousIO.Bean, source uint16, check chan asynchronousIO.Bean) {
	if check == nil {
		err := m.dataSource[source].Save(bean)
		if err != nil {
			return
		}
	} else {
		err := m.dataSource[source].Save(bean)
		if err != nil {
			check <- asynchronousIO.AsynchronousIOError{E: err}
			return
		}
		check <- asynchronousIO.Finish{}
	}
}

func (m *reIn) deleteWorker(key asynchronousIO.Key, source uint16, check chan asynchronousIO.Bean) {
	err := m.dataSource[source].Delete(key)
	if err != nil {
		check <- asynchronousIO.AsynchronousIOError{E: err}
		return
	}
	check <- asynchronousIO.Finish{}
}
