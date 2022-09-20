package main

import (
	"net/http"
	"testing"
)

func TestSearch(t *testing.T) {
	type args struct {
		w   http.ResponseWriter
		req *http.Request
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{name: "test"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Search(tt.args.w, tt.args.req)
		})
	}
}
