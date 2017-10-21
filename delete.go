package main

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

type delObject struct {
	XMLName xml.Name `xml:"Object"`
	Key     string
}

type delResult struct {
	XMLName xml.Name `xml:"DeleteResult"`
	Error   []struct {
		Key, Code, Message string
	}
}

func (b *Bucket) delFiles(files []*File) (statusCode int) {
	if len(files) > 1000 {
		return http.StatusInternalServerError
	}
	delObjects := make([]*delObject, len(files))
	for i, f := range files {
		f.Path = strings.TrimLeft(f.Path, "/")
		delObjects[i] = &delObject{Key: f.Path}
	}
	x, err := xml.Marshal(delObjects)
	if err != nil {
		log.Printf("delFiles XML Marshal: %v\n", err)
		return http.StatusInternalServerError
	}
	body := string(x)
	body = "<Delete>" + body + "</Delete>"
	binBody := []byte(body)

	req, err := http.NewRequest("POST", b.URL+"?delete", bytes.NewReader(binBody))
	if err != nil {
		log.Printf("delFiles NewRequest: %v\n", err)
		return http.StatusInternalServerError
	}
	resp, err := b.run(req, int64(len(binBody)))
	if err != nil {
		log.Printf("delFiles Run: %v\n", err)
		return http.StatusInternalServerError
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("delFiles Resp Read Body: %v\n", err)
		return http.StatusInternalServerError
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("delFiles Resp s3 Error: %v\n", string(respBody))
		return resp.StatusCode
	}
	dr := &delResult{}
	if err = xml.Unmarshal(respBody, dr); err != nil {
		log.Printf("delFiles Resp Body XML Unmarshal: %v: %v\n", string(respBody), err)
		return http.StatusInternalServerError
	}
	if len(dr.Error) == 0 {
		// All objects were successfully deleted
		return http.StatusNoContent
	}
	// Some objects were not deleted. Prepare a response

	return http.StatusNoContent
}

func (b *Bucket) Delete(path string) (statusCode int) {
	if path == "" {
		return http.StatusBadRequest
	}
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

	// Object storage always returns StatusNoContent where file
	// being deleted existed or not. So let's go ahead and request
	// delete to Object Storage regardless whether this is a file,
	// folder or it even does not exist. This will delete the file
	// if it exists

	req, err := http.NewRequest("DELETE", b.URL+path, nil)
	if err != nil {
		log.Printf("Delete File NewRequest: %v: %v\n", path, err)
		return http.StatusInternalServerError
	}
	resp, err := b.run(req, 0)
	if err != nil {
		log.Printf("Delete File Run: %v: %v\n", path, err)
		return http.StatusInternalServerError
	}
	resp.Body.Close()

	// Now let's see if this is a folder
	folder, statusCode := b.Dir(path+DirObjectSuffix, "/", 1)
	if statusCode != http.StatusOK {
		return
	}
	if folder == nil {
		// This is not a folder. Just return with success
		return http.StatusNoContent
	}

	// This is a folder. We need to delete all keys within
	// the folder
	files, statusCode := b.Dir(path+"/", "", 0)
	if statusCode != http.StatusOK {
		return http.StatusInternalServerError
	}
	// We can only delete 1000 keys at a time
	delObjects := make([]DelObject, 1000)
	var pos int
	const batch int = 1000

	for {
		if len(files)-pos > batch {
			fmt.Println(files[pos : pos+batch])
			pos += batch
		} else {
			fmt.Println(files[pos:])
			break
		}
	}

	// If it was not a file or folder, return success
	return http.StatusNoContent
}
