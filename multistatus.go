package main

import "encoding/xml"

type Status struct {
	XMLName xml.Name `xml:"d:response"`
	Href    string   `xml:"d:href"`
	Status  string   `xml:"d:status"`
}

type MultiStatus struct {
	Statuses []*Status
}

func (m *MultiStatus) Add(href, status string) {
	m.Statuses = append(m.Statuses, &Status{Href: href, Status: status})
}

func (m *MultiStatus) Encode() (multiStatus string, err error) {
	x, err := xml.Marshal(m)
	if err != nil {
		return "", err
	}
	return xml.Header + string(x), nil
}
