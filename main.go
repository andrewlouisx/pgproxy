// Copyright 2017 wgliang. All rights reserved.
// Use of this source code is governed by Apache
// license that can be found in the LICENSE file.

// Program pgproxy is a proxy-server to database PostgreSQL.
package main

// import (
// 	"github.com/andrewlouisx/pgproxy/cli"
// )

// func main() {
// 	cli.Main(nil, nil)
// }

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/andrewlouisx/pgproxy/parser"
	"github.com/andrewlouisx/pgproxy/proxy"
)

func main() {
	// Create a new pgproxy instance
	proxy.Start(
		"localhost:5433",
		"localhost:5432",
		loggingHandler,
	)
}

type Metadata struct {
	TransactionId string `json:"transaction_id"`
}

func loggingHandler(input []byte) ([]byte, error) {
	statement, err := parser.Parse(string(input))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	fmt.Println("statement", statement)

	// convert byte slice to string
	rawQuery := string(input)
	metadata, err := getQueryMetadata(rawQuery)
	if err != nil {
		return nil, err
	}

	fmt.Println("metadata", *metadata)
	return input, nil
}

func getQueryMetadata(input string) (*Metadata, error) {
	queryComment, err := extractJSON(input)
	if err != nil {
		return nil, err
	}

	if len(queryComment) == 0 {
		return nil, nil
	}
	metadata, err := unmarshalJSON(queryComment)
	if err != nil {
		return nil, err
	}

	if metadata == nil {
		return nil, nil
	}

	return metadata, nil
}

func extractJSON(input string) (string, error) {
	startIdx := strings.Index(input, "/*")
	endIdx := strings.Index(input, "*/")

	if startIdx != -1 && endIdx != -1 && endIdx > startIdx {
		jsonStr := strings.TrimSpace(input[startIdx+2 : endIdx])
		return jsonStr, nil
	}
	return "", nil
}

func unmarshalJSON(jsonStr string) (*Metadata, error) {
	m := &Metadata{}
	err := json.Unmarshal([]byte(jsonStr), m)
	return m, err
}
