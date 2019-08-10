package M2N

import (
	"asynchronousIO"
	"os"
	"sync"
)

const (
	Load   uint8 = 1
	ToSave uint8 = 1 << iota
	DeleteItem
	saving
)

type CmdTableForUnChangeableDataSource struct {
	table map[int64]*item
}

type item struct {
	bean  asynchronousIO.Bean
	state uint8
	waits []chan asynchronousIO.Bean
}

func (t *CmdTableForUnChangeableDataSource) Save(key int64, bean asynchronousIO.Bean, notify bool, cs *sync.Pool) (c chan asynchronousIO.Bean, start bool) {
	i, ok := t.table[key]
	if ok {
		switch i.state {
		case saving:
			i.state = ToSave
			i.bean = bean
			if notify {
				c = cs.Get().(chan asynchronousIO.Bean)
				i.waits = append(i.waits, c)
			}
		case ToSave: // todo test three save
			if len(i.waits) > 0 {
				i.waits[0] <- asynchronousIO.Finish{}
				i.waits = i.waits[:0]
			}
			i.bean = bean
			if notify {
				c = cs.Get().(chan asynchronousIO.Bean)
				i.waits = append(i.waits, c)
			}
		case Load:
			i.state = ToSave
			i.bean = bean
			for _, v := range i.waits {
				v <- bean
			}
			if notify {
				c = cs.Get().(chan asynchronousIO.Bean)
				i.waits[0] = c
			}
		case DeleteItem:
			return nil, false
		}
	} else {
		t.table[key] = &item{bean, saving, make([]chan asynchronousIO.Bean, 0, 3)}
		if notify {
			c = cs.Get().(chan asynchronousIO.Bean)
		}
		return c, true
	}
	return
}

func (t *CmdTableForUnChangeableDataSource) Load(k int64, cs *sync.Pool) (c chan asynchronousIO.Bean, bean asynchronousIO.Bean) {
	i, ok := t.table[k]
	if ok {
		switch i.state {
		case saving:
			return nil, i.bean
		case ToSave:
			return nil, i.bean
		case Load:
			c = cs.Get().(chan asynchronousIO.Bean)
			i.waits = append(i.waits, c)
			return c, nil
		case DeleteItem:
			return nil, asynchronousIO.AsynchronousIOError{E: &os.PathError{Path: string(k)}}
		}
	} else {
		t.table[k] = &item{bean, Load, make([]chan asynchronousIO.Bean, 0, 3)}
		c = cs.Get().(chan asynchronousIO.Bean)
		bean = asynchronousIO.Start{}
	}
	return
}

func (t *CmdTableForUnChangeableDataSource) Delete(k int64, cs *sync.Pool) (c chan asynchronousIO.Bean, start bool) {
	i, ok := t.table[k]
	if ok {
		switch i.state {
		case saving:
			i.state = DeleteItem
			if len(i.waits) != 0 {
				i.waits[0] <- asynchronousIO.Finish{}
				i.waits = i.waits[0:0]
			}
			c = cs.Get().(chan asynchronousIO.Bean)
			i.waits = append(i.waits, c)
			return c, false
		case ToSave: // test two save and delete
			i.state = DeleteItem
			if len(i.waits) != 0 {
				i.waits[0] <- asynchronousIO.Finish{}
				i.waits = i.waits[0:0]
			}
			c = cs.Get().(chan asynchronousIO.Bean)
			i.waits = append(i.waits, c)
			return c, false
		case Load:
			i.state = DeleteItem
			for _, v := range i.waits {
				v <- asynchronousIO.AsynchronousIOError{E: &os.PathError{}}
				i.waits = i.waits[0:0]
			}
			c = cs.Get().(chan asynchronousIO.Bean)
			i.waits = append(i.waits, c)
			return c, false
		case DeleteItem:
			return nil, false
		}
	} else {
		t.table[k] = &item{nil, DeleteItem, nil}
		c = cs.Get().(chan asynchronousIO.Bean)
		start = true
	}

	return
}

func (t *CmdTableForUnChangeableDataSource) Get(k int64, lastCmd uint8, lastBean asynchronousIO.Bean) (cmd uint8, bean asynchronousIO.Bean, c chan asynchronousIO.Bean, restart bool) {
	i := t.table[k]
	//fmt.Println(t.table)
	switch lastCmd {
	case Load:
		if i.state == Load {
			for _, v := range i.waits {
				v <- lastBean
			}
			delete(t.table, k)
			return Load, nil, nil, false
		} else if i.state == ToSave {
			i.state = saving
			if len(i.waits) > 0 {
				w := i.waits[0]
				i.waits = i.waits[:0]
				return ToSave, i.bean, w, true
			}
			return ToSave, i.bean, nil, true
		} else if i.state == DeleteItem {
			w := i.waits[0]
			i.waits = i.waits[:0]
			return DeleteItem, i.bean, w, true
		}
	case ToSave:
		if i.state == ToSave {
			i.state = saving
			if len(i.waits) > 0 {
				w := i.waits[0]
				i.waits = i.waits[:0]
				return ToSave, i.bean, w, true
			}
			return ToSave, i.bean, nil, true
		} else if i.state == DeleteItem {
			w := i.waits[0]
			i.waits = i.waits[:0]
			return DeleteItem, i.bean, w, true
		} else if i.state == saving {
			delete(t.table, k)
			return saving, nil, nil, false
		}
	case DeleteItem:
		delete(t.table, k)
		return DeleteItem, nil, nil, false
	}
	return lastCmd, nil, nil, false
}
