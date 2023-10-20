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
	"fmt"

	"github.com/andrewlouisx/pgproxy/parser"
	"github.com/andrewlouisx/pgproxy/proxy"
)

func main() {
	// Create a new pgproxy instance
	proxy.Start(
		"localhost:5433",
		"localhost:5432",
		parseRequest,
		parseResponse,
	)
}

func parseRequest(input []byte) bool {
	statement, err := parser.Parse(parser.Extracte(input))
	if err != nil {
		fmt.Println(err)
	}
	println("request", statement)
	return true
}

func parseResponse(input []byte) bool {
	statement, err := parser.Parse(parser.Extracte(input))
	if err != nil {
		fmt.Println(err)
	}
	println("response", statement)
	return true
}
