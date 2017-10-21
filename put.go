package main

import (
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

// Put creates a file on object storage
func (b *Bucket) Put(path string, content io.ReadCloser, size int64) (statusCode int) {

	if path == "" || path == "/" {
		return http.StatusBadRequest
	}
	// There must not be a trailing /
	if strings.HasSuffix(path, "/") {
		path = strings.TrimRight(path, "/")
	}
	// There must be a / prefix
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	// Check if folder with same name exists
	folder, statusCode := b.Dir(path+DirObjectSuffix, "/", 1)
	if statusCode != http.StatusOK {
		return
	}
	if folder != nil {
		return http.StatusMethodNotAllowed
	}

	// Check if parent exists
	parent := parent(path)
	if parent != "/" {
		folder, statusCode = b.Dir(parent+DirObjectSuffix, "/", 1)
		if statusCode != http.StatusOK {
			return
		}
		if folder == nil {
			return http.StatusMethodNotAllowed
		}
	}

	// PUT the object
	req, err := http.NewRequest("PUT", b.URL+path, content)
	if err != nil {
		log.Println(err)
		return http.StatusInternalServerError
	}
	resp, err := b.run(req, size)
	if err != nil {
		log.Println(err)
		return http.StatusInternalServerError
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println(err)
			return http.StatusInternalServerError
		}
		log.Println(string(body))
	}
	return resp.StatusCode
}

// PutRaw creates a file on object storage
func (b *Bucket) PutRaw(path string, content io.ReadCloser, size int64) (statusCode int) {

	req, err := http.NewRequest("PUT", b.URL+path, content)
	if err != nil {
		log.Println(err)
		return http.StatusInternalServerError
	}
	resp, err := b.run(req, size)
	if err != nil {
		log.Println(err)
		return http.StatusInternalServerError
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println(err)
			return http.StatusInternalServerError
		}
		log.Println(string(body))
	}
	return resp.StatusCode
}
