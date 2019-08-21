package DataSources

import (
	"github.com/ChenXingyuChina/asynchronousIO"
	"io"
	"os"
)

type FileSystem struct {
	root   string
	models []asynchronousIO.Bean
	news   []func() asynchronousIO.Bean
	makes  []func(bean asynchronousIO.Bean, r io.Reader) error
	saves  []func(bean asynchronousIO.Bean, w io.Writer) error
	check  func(key asynchronousIO.Key) uint16
}

func (s *FileSystem) Load(key asynchronousIO.Key) (asynchronousIO.Bean, error) {
	k, _ := key.ToString()
	f, err := os.Open(s.root + k)
	if err != nil {
		return nil, err
	}
	t := s.check(key)
	m := s.news[t]()
	err = s.makes[t](m, f)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (s *FileSystem) Save(bean asynchronousIO.Bean) error {
	key := bean.GetKey()
	k, _ := key.ToString()
	f, err := os.Create(s.root + k)
	if err != nil {
		return err
	}
	t := s.check(key)
	err = s.saves[t](bean, f)
	return err
}

func (s *FileSystem) Delete(key asynchronousIO.Key) error {
	n, _ := key.ToString()
	return os.Remove(s.root + n)
}

func NewFileSystemDataSource(root string,
	models []asynchronousIO.Bean,
	newFunctions []func() asynchronousIO.Bean,
	makeFunctions []func(bean asynchronousIO.Bean, r io.Reader) error,
	saveFunctions []func(bean asynchronousIO.Bean, w io.Writer) error) asynchronousIO.DataSource {
	for _, v := range models {
		if _, ok := v.GetKey().ToString(); !ok {
			panic("please use a beans whose key's ToString function return true")
		}
	}
	return &FileSystem{root: root, models: models, makes: makeFunctions, news: newFunctions, saves: saveFunctions}
}
