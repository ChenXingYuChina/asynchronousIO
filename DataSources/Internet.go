package DataSources

import (
	"asynchronousIO"
	"bytes"
	"io"
	"net/http"
)

type Http struct {
	domain      string
	contentType string
	models      []asynchronousIO.Bean
	news        []func() asynchronousIO.Bean
	makes       []func(bean asynchronousIO.Bean, r io.Reader) error
	toByteArray []func(bean asynchronousIO.Bean) []byte
	check       func(key asynchronousIO.Key) uint16
}

func (s *Http) Load(key asynchronousIO.Key) (asynchronousIO.Bean, error) {
	k, _ := key.ToString()
	f, err := http.Get(s.domain + k)
	if err != nil {
		return nil, err
	}
	t := s.check(key)
	m := s.news[t]()
	err = s.makes[t](m, f.Body)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (s *Http) Save(bean asynchronousIO.Bean) error {
	key := bean.GetKey()
	k, _ := key.ToString()
	t := s.check(key)
	request, err := http.NewRequest("PUT", s.domain+k, bytes.NewBuffer(s.toByteArray[t](bean)))
	if err != nil {
		return err
	}
	request.Header.Add("content-type", s.contentType)
	res, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	return res.Body.Close()
}

func (s *Http) Delete(key asynchronousIO.Key) error {
	k, _ := key.ToString()
	request, err := http.NewRequest("DELETE", s.domain+k, nil)
	if err != nil {
		return err
	}
	request.Header.Add("content-type", s.contentType)
	res, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	return res.Body.Close()
}

func (s *Http) CanReIn() bool {
	return true
}
