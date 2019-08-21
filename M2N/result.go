package M2N

import (
	"github.com/ChenXingyuChina/asynchronousIO"
	"sync"
)

var resultPool = new(sync.Pool)

func init() {
	resultPool.New = func() interface{} {
		return &result{}
	}
}

type result struct {
	c       chan asynchronousIO.Bean
	cBuffer *sync.Pool
}

func (r *result) get() (asynchronousIO.Bean, error) {
	bean := <-r.c
	r.cBuffer.Put(r.c)
	resultPool.Put(r)
	if err, is := bean.(asynchronousIO.AsynchronousIOError); is {
		return nil, err.E
	}
	return bean, nil
}

func (r *result) error() error {
	bean := <-r.c
	r.cBuffer.Put(r.c)
	resultPool.Put(r)
	if err, is := bean.(asynchronousIO.AsynchronousIOError); is {
		return err.E
	}
	return nil
}

func genResult(c chan asynchronousIO.Bean, buffer *sync.Pool) *result {
	goal := resultPool.Get().(*result)
	goal.cBuffer = buffer
	goal.c = c
	return goal
}
