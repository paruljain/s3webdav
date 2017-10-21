package main

import (
	"log"
	"net/http"
	"strconv"
	"sync"
)

// File stores properties of a file or folder on object storage
type File struct {
	Path         string `xml:"Key"`
	LastModified string
	IsDir        bool
	ETag         string
	Size         int64
}

// Stat returns properties of a file or folder on object storage
func (b *Bucket) Stat(path string) *File {
	var file *File
	var folder *File
	wg := &sync.WaitGroup{}

	// Test for file
	wg.Add(1)
	go func() {
		defer wg.Done()
		req, err := http.NewRequest("HEAD", b.URL+path, nil)
		if err != nil {
			log.Println(err)
			return
		}
		resp, err := b.run(req, 0)
		if err != nil {
			log.Println(err)
			return
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			// This is a file
			size, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
			if err != nil {
				log.Printf("Stat: Unable to parse Content-Length to int64: %v: %v: %v\n",
					path, resp.Header.Get("Content-Length"), err)
				return
			}
			file = &File{
				Path:         path,
				LastModified: resp.Header.Get("Last-Modified"),
				ETag:         resp.Header.Get("ETag"),
				Size:         size,
				IsDir:        false,
			}
			return
		}
	}()

	// Test for folder
	wg.Add(1)
	go func() {
		defer wg.Done()
		req, err := http.NewRequest("HEAD", b.URL+path+" d", nil)
		if err != nil {
			log.Println(err)
			return
		}
		resp, err := b.run(req, 0)
		if err != nil {
			log.Println(err)
			return
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			folder = &File{
				Path:         path,
				LastModified: resp.Header.Get("Last-Modified"),
				ETag:         resp.Header.Get("ETag"),
				Size:         0,
				IsDir:        true,
			}
		}
		return
	}()
	wg.Wait()
	if file != nil {
		return file
	}
	return folder
}

// Statd returns properties of a folder on object storage
func (b *Bucket) Statd(path string) (file *File) {
	req, err := http.NewRequest("HEAD", b.URL+path+" d", nil)
	if err != nil {
		log.Println(err)
		return
	}
	resp, err := b.run(req, 0)
	if err != nil {
		log.Println(err)
		return
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		file = &File{
			Path:         path,
			LastModified: resp.Header.Get("Last-Modified"),
			ETag:         resp.Header.Get("ETag"),
			Size:         0,
			IsDir:        true,
		}
	}
	return
}
