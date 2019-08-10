package M2N

import (
	"asynchronousIO"
	"runtime"
	"sync"
)

type normal struct {
	inUse             []asynchronousIO.CmdTable
	locks             []*sync.Mutex
	channelBuffer     *sync.Pool
	dataSource        []asynchronousIO.DataSource
	typeNumber        int64
	bufferBlockNumber int64
}

func (m *normal) Load(key asynchronousIO.Key, source uint16) func() (asynchronousIO.Bean, error) {
	k, ok := key.UniqueId()
	if !ok {
		panic("you must use a key whose UniqueId function returns a true")
	}
	p := k%m.bufferBlockNumber + m.bufferBlockNumber*key.TypeId()
	lock := m.locks[p]
	lock.Lock()
	cmdTable := m.inUse[p]
	c, bean := cmdTable.Load(k, m.channelBuffer)
	lock.Unlock()
	if _, ok := bean.(asynchronousIO.Start); ok {
		go m.loadWorker(key, k, c, m.dataSource[source])
		//fmt.Println("load start")
		return result{c: c, cBuffer: m.channelBuffer}.get
	}
	if c == nil {
		return func(b asynchronousIO.Bean) func() (bean asynchronousIO.Bean, e error) {
			return func() (bean asynchronousIO.Bean, e error) {
				if v, ok := b.(asynchronousIO.AsynchronousIOError); ok {
					return nil, v
				}
				return b, nil
			}
		}(bean)

	}
	return result{c: c, cBuffer: m.channelBuffer}.get
}

func (m *normal) Save(bean asynchronousIO.Bean, source uint16) {
	key := bean.GetKey()
	k, ok := key.UniqueId()
	if !ok {
		panic("you must use a key whose UniqueId function returns a true")
	}
	p := k%m.bufferBlockNumber + m.bufferBlockNumber*key.TypeId()
	lock := m.locks[p]
	lock.Lock()
	cmdTable := m.inUse[p]
	_, start := cmdTable.Save(k, bean, false, m.channelBuffer)
	lock.Unlock()
	if start {
		go m.saveWorker(nil, key, k, bean, m.dataSource[source])
	}
}

func (m *normal) Delete(key asynchronousIO.Key, source uint16) func() error {
	k, ok := key.UniqueId()
	if !ok {
		panic("you must use a key whose UniqueId function returns a true")
	}
	p := k%m.bufferBlockNumber + m.bufferBlockNumber*key.TypeId()
	lock := m.locks[p]
	lock.Lock()
	cmdTable := m.inUse[p]
	c, start := cmdTable.Delete(k, m.channelBuffer)
	lock.Unlock()
	//fmt.Println("delete")
	if start {
		go m.deleteWorker(key, k, c, m.dataSource[source])
	}
	if c == nil {
		return func() (e error) {
			return nil
		}
	}
	return result{c, m.channelBuffer}.error
}

func (m *normal) SaveAndCallBackWhenFinish(bean asynchronousIO.Bean, source uint16) func() error {
	key := bean.GetKey()
	k, ok := key.UniqueId()
	if !ok {
		panic("you must use a key whose UniqueId function returns a true")
	}
	p := k%m.bufferBlockNumber + m.bufferBlockNumber*key.TypeId()
	lock := m.locks[p]
	lock.Lock()
	cmdTable := m.inUse[p]
	c, start := cmdTable.Save(k, bean, true, m.channelBuffer)
	lock.Unlock()
	if c == nil {
		return func() (e error) {
			return nil
		}
	}
	if start {
		go m.saveWorker(c, key, k, bean, m.dataSource[source])
	}
	return result{c, m.channelBuffer}.error
}

func newNormal(source []asynchronousIO.DataSource, beanTypeNumber int64, expectBufferNumber int64) asynchronousIO.AsynchronousIOMachine {
	blockNumber := int64(runtime.NumCPU() * 2)
	if blockNumber <= expectBufferNumber {
		blockNumber = expectBufferNumber
	}
	if blockNumber%beanTypeNumber != 0 {
		blockNumber += beanTypeNumber - blockNumber%beanTypeNumber
	}
	table := make([]asynchronousIO.CmdTable, blockNumber)
	locks := make([]*sync.Mutex, blockNumber)
	for i, _ := range table {
		table[i] = &CmdTableForUnChangeableDataSource{make(map[int64]*item, 20)}
		locks[i] = new(sync.Mutex)
	}
	pool := new(sync.Pool)
	pool.New = func() interface{} {
		return make(chan asynchronousIO.Bean, 1)
	}
	return &normal{inUse: table, channelBuffer: pool, dataSource: source, bufferBlockNumber: blockNumber / beanTypeNumber, locks: locks}
}

func (m *normal) loadWorker(key asynchronousIO.Key, k int64, c chan asynchronousIO.Bean, dataSource asynchronousIO.DataSource) {
	p := k%m.bufferBlockNumber + m.bufferBlockNumber*key.TypeId()
	lock := m.locks[p]
	table := m.inUse[p]
	cmd := Load
	model, err := dataSource.Load(key)
	if err != nil {
		c <- asynchronousIO.AsynchronousIOError{err}
		goto next
	}
	c <- model
next:
	lock.Lock()
	var restart bool
	cmd, model, c, restart = table.Get(k, cmd, model)
	//fmt.Println(cmd, model, c, restart)
	if !restart {
		lock.Unlock()
		return
	}
	lock.Unlock()
	if cmd == ToSave {
		if c != nil {
			err := dataSource.Save(model)
			if err != nil {
				c <- asynchronousIO.AsynchronousIOError{err}
				goto next
			}
			c <- asynchronousIO.Finish{}
		} else {
			//fmt.Println(model)
			err := dataSource.Save(model)
			if err != nil {
				goto next
			}
		}
	} else if cmd == DeleteItem {
		err = dataSource.Delete(key)
		if err != nil {
			c <- asynchronousIO.AsynchronousIOError{err}
			goto next
		}
		c <- asynchronousIO.Finish{}
	}
	goto next
}

func (m *normal) saveWorker(c chan asynchronousIO.Bean, key asynchronousIO.Key, k int64, bean asynchronousIO.Bean, dataSource asynchronousIO.DataSource) {
	p := k%m.bufferBlockNumber + m.bufferBlockNumber*key.TypeId()
	lock := m.locks[p]
	table := m.inUse[p]
	cmd := ToSave
	if c != nil {
		err := dataSource.Save(bean)
		if err != nil {
			c <- asynchronousIO.AsynchronousIOError{err}
			goto next
		}
		c <- asynchronousIO.Finish{}
	} else {
		err := dataSource.Save(bean)
		if err != nil {
			goto next
		}
	}
next:
	lock.Lock()
	var model asynchronousIO.Bean
	var restart bool
	cmd, model, c, restart = table.Get(k, cmd, bean)
	if !restart {
		lock.Unlock()
		return
	}
	lock.Unlock()
	if cmd == ToSave {
		if c != nil {
			err := dataSource.Save(model)
			if err != nil {
				c <- asynchronousIO.AsynchronousIOError{err}
				goto next
			}
			c <- asynchronousIO.Finish{}
		} else {
			err := dataSource.Save(model)
			if err != nil {
				goto next
			}
		}
	} else if cmd == DeleteItem {
		err := dataSource.Delete(key)
		if err != nil {
			c <- asynchronousIO.AsynchronousIOError{err}
			goto next
		}
		c <- asynchronousIO.Finish{}
	}
	goto next
}

func (m *normal) deleteWorker(key asynchronousIO.Key, k int64, c chan asynchronousIO.Bean, dataSource asynchronousIO.DataSource) {
	p := k%m.bufferBlockNumber + m.bufferBlockNumber*key.TypeId()
	err := dataSource.Delete(key)
	if err != nil {
		c <- asynchronousIO.AsynchronousIOError{err}
	} else {
		c <- asynchronousIO.Finish{}
	}
	m.locks[p].Lock()
	m.inUse[p].Get(k, DeleteItem, nil)
	m.locks[p].Unlock()
}
