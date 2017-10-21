package main

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

type Bucket struct {
	AccessKey string
	SecretKey string
	URL       string
	Client    *http.Client
}

func NewBucket(ep string, accessKey string, secretKey string, bucketName string) *Bucket {
	return &Bucket{
		AccessKey: accessKey,
		SecretKey: secretKey,
		URL:       ep + "/" + bucketName,
		Client:    &http.Client{},
	}
}

func splitPath(path string) (folder string, filename string, err error) {
	path = strings.TrimSpace(path)
	if path == "" {
		err = errors.New("Path is empty")
		return
	}
	if !strings.HasPrefix(path, "/") {
		err = errors.New("Path must start with /")
		return
	}
	if path == "/" {
		folder = "/"
		return
	}

	if strings.HasSuffix(path, "/") {
		err = errors.New("Path must not have a trailing /")
		return
	}
	parts := strings.Split(path, "/")
	if len(parts) == 2 {
		folder = "/"
		filename = parts[1]
		return
	}
	folder = "/" + strings.Join(parts[1:len(parts)-1], "/")
	filename = parts[len(parts)-1]
	return
}

func (b *Bucket) run(req *http.Request, size int64) (resp *http.Response, err error) {
	req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	req.ContentLength = size
	b.sign(req)
	resp, err = b.Client.Do(req)
	return
}

type S3Error struct {
	StatusCode int
	Code       string
	Message    string
	Key        string
	BucketName string
	Resource   string
	RequestId  string
	HostId     string
}

/*
func (b *Bucket) Delete(path string) (statusCode int) {
	req, err := http.NewRequest("DELETE", b.URL+"/"+key, nil)
	if err != nil {
		log.Println(err)
		return http.StatusInternalServerError
	}
	resp, err := b.run(req, 0)
	if err != nil {
		log.Println(err)
		return http.StatusInternalServerError
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println(err)
			return http.StatusInternalServerError
		}
		log.Println(string(body))
	}
	return resp.StatusCode
}
*/

var debug bool

const Infinity int = -1

func main() {
	debug = false
	//b := NewBucket("http://127.0.0.1:9000", "82Z7UVWLMN7K4TMP9RJF",
	//	"b9PbWQoLVCHT1vq1LaE6twQIKUr3y0ArvckDSrr9", "test")

	b := NewBucket("http://127.0.0.1:9000", "LKKB31TCA0VVORCCZI6Y",
		"5PJ7uKvsDbPFsmI2mYSFWZBtctJ726yV6MGEHCxY", "test")

	f, err := os.Open("main.go")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(b.Put("/project1/docs/test.go", f, fi.Size()))

	//fmt.Println(b.Mkdir("/project1/data"))

	files, code := b.Dir("/", "", 0)
	fmt.Println(code)
	for _, f := range files {
		fmt.Println(f)
	}

	//statusCode := b.MkCol("/folder1")
	//fmt.Println(statusCode)

	//statusCode = b.MkCol("/folder2")
	//fmt.Println(statusCode)
	//statusCode = b.MkCol("/folder1/folder11")
	//fmt.Println(statusCode)
	//fmt.Println(b.Stat("/folder1/xxx"))
	//fmt.Println(b.Stat("/folder1/folder11/hmmm"))
	//statusCode = b.MkCol("/mydocuments/project1/templates")
	//fmt.Println(statusCode)
	//b.IsDir("/")
	//if statusCode != http.StatusOK {
	//	fmt.Println(statusCode)
	//}
}
