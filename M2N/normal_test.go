package M2N

import (
	"asynchronousIO"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

type testBean struct {
	data byte
	key  int64
}

func (b *testBean) GetKey() asynchronousIO.Key {
	return testKey(b.key)
}

type testKey int64

func (k testKey) TypeId() int64 {
	return 0
}

func (k testKey) UniqueId() (int64, bool) {
	return int64(k), true
}

func (testKey) ToString() (string, bool) {
	panic("implement me")
}

var m asynchronousIO.AsynchronousIOMachine
var m0, m1 []byte

func init() {
	m0 = make([]byte, 1000)
	m1 = make([]byte, 1000)
	m = newNormal([]asynchronousIO.DataSource{testDataSource(m0), testDataSource(m1)},
		1, 10)
}

type testDataSource []byte

func (m testDataSource) Load(key asynchronousIO.Key) (asynchronousIO.Bean, error) {
	if v, _ := key.UniqueId(); v > int64(len(m)) {
		return nil, asynchronousIO.AsynchronousIOError{errors.New("over bound")}
	} else {
		return &testBean{m[v], v}, nil
	}
}

func (m testDataSource) Save(bean asynchronousIO.Bean) error {
	b := bean.(*testBean)
	if b.key > int64(len(m)) {
		return asynchronousIO.AsynchronousIOError{errors.New("over bound")}
	}
	m[b.key] = b.data
	return nil
}
func (m testDataSource) Delete(key asynchronousIO.Key) error {
	start, _ := key.UniqueId()
	if start > int64(len(m)) {
		return asynchronousIO.AsynchronousIOError{errors.New("over bound")}
	}
	m[start] = 0
	return nil
}

func TestWithCheck_Load(t *testing.T) {
	m0[0] = 10
	f := m.Load(testKey(0), 0)
	b, err := f()
	if (b.(*testBean)).data != 10 {
		fmt.Println(err, b)
		t.Fatal()
	}
}

func TestWithCheck_Save(t *testing.T) {
	b := &testBean{2, 10}
	m.Save(b, 1)
	<-time.Tick(1 * time.Second)
	if m1[10] != 2 {
		t.Fatal()
	}
}

func TestWithCheck_Delete(t *testing.T) {
	m1[10] = 2
	f := m.Delete(testKey(10), 1)
	err := f()
	if err != nil || m1[10] != 0 {
		fmt.Println(err, m1[10])
		t.Fatal()
	}
}

func TestWithCheck_SaveAndCallBackWhenFinish(t *testing.T) {
	m0[5] = 0
	//<-time.Tick(1*time.Second)
	f := m.SaveAndCallBackWhenFinish(&testBean{10, 5}, 0)
	err := f()
	if err != nil || m0[5] != 10 {
		fmt.Println(err, m0[5])
		t.Fatal()
	}
}

func TestWithCheck_DeleteAndLoad(t *testing.T) {
	m0[5] = 1
	f := m.Delete(testKey(5), 0)
	f2 := m.Load(testKey(5), 0)
	b, err := f2()
	if e, ok := err.(asynchronousIO.AsynchronousIOError); !ok {
		t.Fatal()
	} else {
		if _, ok := e.E.(*os.PathError); !ok {
			t.Fatal()
		}
	}
	if b != nil {
		t.Fatal()
	}
	err = f()
	if err != nil || m0[5] != 0 {
		t.Fatal()
	}
}

func TestWithCheck_DeleteAndSave(t *testing.T) {
	m0[5] = 1
	f := m.Delete(testKey(5), 0)
	m.Save(&testBean{10, 5}, 0)
	//b, err := f2()
	//if e, ok := err.(asynchronousIO.AsynchronousIOError); !ok {
	//	t.Fatal()
	//} else {
	//	if _, ok := e.E.(*os.PathError); !ok {
	//		t.Fatal()
	//	}
	//}
	//if b != nil {
	//	t.Fatal()
	//}
	err := f()
	if err != nil || m0[5] != 0 {
		t.Fatal()
	}
}

func TestWithCheck_DeleteThenSaveAndCallBackWhenFinish(t *testing.T) {
	m0[5] = 1
	f := m.Delete(testKey(5), 0)
	f2 := m.SaveAndCallBackWhenFinish(&testBean{10, 5}, 0)
	err := f2()
	if err != nil {
		t.Fatal()
	}
	err = f()
	if err != nil || m0[5] != 0 {
		t.Fatal()
	}
}

func TestWithCheck_DeleteAndDelete(t *testing.T) {
	m0[5] = 1
	f := m.Delete(testKey(5), 0)
	f2 := m.Delete(testKey(5), 0)
	err := f2()
	if err != nil {
		t.Fatal()
	}
	err = f()
	if err != nil || m0[5] != 0 {
		t.Fatal()
	}
}

func TestWithCheck_LoadAndLoad(t *testing.T) {
	m0[10] = 10
	f := m.Load(testKey(10), 0)
	f2 := m.Load(testKey(10), 0)
	b, err := f2()
	if err != nil {
		t.Fatal()
	}
	if v, ok := b.(*testBean); ok {
		if v.data != 10 {
			fmt.Println(v)
			t.Fatal()
		}
	} else {
		fmt.Println(b)
		t.Fatal()
	}

	b, err = f()
	if err != nil {
		t.Fatal()
	}
	if v, ok := b.(*testBean); ok {
		if v.data != 10 {
			t.Fatal()
		}
	} else {
		t.Fatal()
	}
	if err != nil {
		t.Fatal()
	}
}

func TestWithCheck_LoadAndSave(t *testing.T) {
	m0[5] = 1
	f := m.Load(testKey(5), 0)
	m.Save(&testBean{key: 5, data: 10}, 0)
	b, err := f()
	if err != nil {
		t.Fatal()
	}
	if v, ok := b.(*testBean); ok {
		if v.data != 1 {
			t.Fatal()
		}
	} else {
		t.Fatal()
	}
	<-time.Tick(1 * time.Second)
	if m0[5] != 10 {
		t.Fatal()
	}
}

func TestWithCheck_LoadAndDelete(t *testing.T) {
	m0[5] = 1
	f := m.Load(testKey(5), 0)
	f2 := m.Delete(testKey(5), 0)
	b, err := f()
	if err != nil {
		t.Fatal()
	}
	if v, ok := b.(*testBean); ok {
		if v.data != 1 {
			t.Fatal()
		}
	} else {
		t.Fatal()
	}
	err = f2()
	if err != nil {
		t.Fatal()
	}
	<-time.Tick(1 * time.Second)
	if m0[5] != 0 {
		t.Fatal()
	}
}

func TestWithCheck_SaveAndLoad(t *testing.T) {
	m0[10] = 0
	f := m.SaveAndCallBackWhenFinish(&testBean{key: 10, data: 5}, 0)
	f2 := m.Load(testKey(10), 0)
	b, err := f2()
	if err != nil {
		t.Fatal()
	}
	if v, ok := b.(*testBean); b == nil || !ok {
		t.Fatal()
	} else {
		if v.data != 5 {
			t.Fatal()
		}
	}
	err = f()
	if err != nil || m0[10] != 5 {
		t.Fatal()
	}
}

func TestWithCheck_SaveAndSave(t *testing.T) {
	m0[10] = 0
	f := m.SaveAndCallBackWhenFinish(&testBean{key: 10, data: 5}, 0)
	f2 := m.SaveAndCallBackWhenFinish(&testBean{key: 10, data: 6}, 0)
	err := f2()
	if err != nil {
		t.Fatal()
	}
	err = f()
	if err != nil || m0[10] != 6 {
		t.Fatal()
	}
}

func TestWithCheck_SaveAndDelete(t *testing.T) {
	m0[10] = 0
	f := m.SaveAndCallBackWhenFinish(&testBean{key: 10, data: 5}, 0)
	f2 := m.Delete(testKey(10), 0)
	err := f2()
	if err != nil {
		t.Fatal()
	}
	err = f()
	if err != nil || m0[10] != 0 {
		t.Fatal()
	}
}

func TestWithCheck_SaveSaveAndSave(t *testing.T) {
	m0[10] = 0
	f := m.SaveAndCallBackWhenFinish(&testBean{key: 10, data: 5}, 0)
	f2 := m.SaveAndCallBackWhenFinish(&testBean{key: 10, data: 6}, 0)
	f3 := m.SaveAndCallBackWhenFinish(&testBean{key: 10, data: 7}, 0)
	err := f2()
	if err != nil {
		t.Fatal()
	}
	err = f3()
	if err != nil {
		t.Fatal()
	}
	err = f()
	if err != nil || m0[10] != 7 {
		t.Fatal()
	}
}

func TestWithCheck_SaveSaveAndDelete(t *testing.T) {
	m0[10] = 0
	f := m.SaveAndCallBackWhenFinish(&testBean{key: 10, data: 5}, 0)
	f2 := m.SaveAndCallBackWhenFinish(&testBean{key: 10, data: 6}, 0)
	f3 := m.Delete(testKey(10), 0)
	err := f2()
	if err != nil {
		t.Fatal()
	}
	err = f3()
	if err != nil {
		t.Fatal()
	}
	err = f()
	if err != nil || m0[10] != 0 {
		t.Fatal()
	}
}
