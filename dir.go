package main

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

// File stores properties of a file or folder on object storage
type File struct {
	Path         string `xml:"Key"`
	LastModified string
	IsDir        bool
	ETag         string
	Size         int64
}

type listResponse struct {
	Name, Prefix, Marker, NextMarker string
	MaxKeys                          int
	IsTruncated                      bool
	Contents                         []*File
	CommonPrefixes                   []string
}

// Dir returns list of files on object storage
func (b *Bucket) Dir(prefix, delimiter string, maxKeys int) (files []*File, statusCode int) {
	var marker string
	for {
		lr, statusCode := b.dirRaw(prefix, delimiter, maxKeys, marker)
		if statusCode != http.StatusOK {
			return nil, http.StatusInternalServerError
		}
		files = append(files, lr.Contents...)
		if !lr.IsTruncated {
			break
		}
		// We need to run again to get the next batch of keys
		if lr.NextMarker == "" {
			// Delimiter was not specified so NextMarker is blank
			// In this case use the last key in the response as marker
			marker = lr.Contents[len(lr.Contents)-1].Path
		} else {
			marker = lr.NextMarker
		}
	}
	return files, http.StatusOK
}

// DirRaw get a listing of keys on object storage
func (b *Bucket) dirRaw(prefix string, delimiter string, maxKeys int, marker string) (lr *listResponse, statusCode int) {
	if prefix == "/" {
		prefix = ""
	}

	req, err := http.NewRequest("GET", b.URL, nil)
	if err != nil {
		log.Printf("DirRaw NewRequest: %v: %v\n", prefix, err)
		return nil, http.StatusInternalServerError
	}
	q := req.URL.Query()
	if prefix != "" {
		q.Set("prefix", prefix)
	}
	if delimiter != "" {
		q.Set("delimiter", delimiter)
	}
	if marker != "" {
		q.Set("marker", marker)
	}
	if maxKeys > 0 && maxKeys < 1001 {
		q.Set("max-keys", fmt.Sprint(maxKeys))
	}

	req.URL.RawQuery = q.Encode()
	resp, err := b.run(req, 0)
	if err != nil {
		log.Printf("DirRaw Run: %v: %v\n", req.URL, err)
		return nil, http.StatusInternalServerError
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("DirRaw Read Body: %v: %v\n", req.URL, err)
		return nil, http.StatusInternalServerError
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("DirRaw s3 error: %v: %v\n", req.URL, string(body))
		return nil, resp.StatusCode
	}
	lr = &listResponse{}
	if err = xml.Unmarshal(body, lr); err != nil {
		log.Printf("DirRaw Unmarshal: %v: %v: %v\n", req.URL, string(body), err)
		return nil, http.StatusInternalServerError
	}
	// Update folder names and IsDir before returning
	for _, f := range lr.Contents {
		if !strings.HasPrefix(f.Path, "/") {
			f.Path = "/" + f.Path
		}
		if strings.HasSuffix(f.Path, DirObjectSuffix) {
			f.Path = strings.Replace(f.Path, DirObjectSuffix, "", 1)
			f.IsDir = true
		}
	}
	return lr, http.StatusOK
}
