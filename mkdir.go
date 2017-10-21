package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

func parent(path string) string {
	if path == "" || path == "/" {
		return ""
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if strings.HasSuffix(path, "/") {
		path = strings.TrimRight(path, "/")
	}
	parts := strings.Split(path, "/")[1:]
	return "/" + strings.Join(parts[:len(parts)-1], "/")
}

const DirObjectSuffix = "-dir-e7588936-193f-4f63-9de4-ecaff18c5d8a"

// Mkdir creates a folder on object storage
func (b *Bucket) Mkdir(path string) (statusCode int) {

	if path == "" {
		return http.StatusBadRequest
	}
	// Reject if mkdir /
	if path == "/" {
		return http.StatusForbidden
	}
	// There must not be a trailing /
	if strings.HasSuffix(path, "/") {
		path = strings.TrimRight(path, "/")
	}
	// There must be a / prefix
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	// Check if file with same name exists
	file, statusCode := b.Dir(path, "/", 1)
	if statusCode != http.StatusOK {
		return
	}
	if file != nil {
		return http.StatusMethodNotAllowed
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

	// Create folder
	req, err := http.NewRequest("PUT", b.URL+path+DirObjectSuffix, nil)
	if err != nil {
		log.Printf("Mkdir Newrequest: %v\n", err)
		return http.StatusInternalServerError
	}
	resp, err := b.run(req, 0)
	if err != nil {
		log.Printf("Mkdir run: %v\n", err)
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
		return http.StatusInternalServerError
	}
	return http.StatusCreated
}
