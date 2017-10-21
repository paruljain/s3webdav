package main

import (
	"encoding/xml"
	"fmt"
)

type FilePropsResponse struct {
	Response struct {
		Href     string `xml:"d:href"`
		PropStat struct {
			Prop struct {
				GetContentLanguage string `xml:"d:getcontentlanguage"`
				GetContentLength   int64  `xml:"d:getcontentlength"`
				GetContentType     string `xml:"d:getcontenttype"`
				GetETag            string `xml:"d:getetag"`
				GetLastModified    string `xml:"d:getlastmodified"`
				ResourceType       string `xml:"d:resourcetype"`
			} `xml:"d:prop"`
			Status string `xml:"d:status"`
		} `xml:"d:propstat"`
	} `xml:"d:response"`
}

type FileProps struct {
	Fpr []*FilePropsResponse
}

func (p *FileProps) Add(contentLength int64, eTag string, lastModified string) {
	fpr := &FilePropsResponse{}
	fpr.Response.Href = "http://foo.bar/test/main.go"
	fpr.Response.PropStat.Prop.GetContentLanguage = "none"
	fpr.Response.PropStat.Prop.GetContentLength = 1234
	p.Fpr = append(p.Fpr, fpr)
}

func (p *FileProps) Encode() {
	x, err := xml.Marshal(p.Fpr)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(x))
}
