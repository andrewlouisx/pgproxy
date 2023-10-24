// Copyright 2017 wgliang. All rights reserved.
// Use of this source code is governed by Apache
// license that can be found in the LICENSE file.

// Package proxy provides proxy service and redirects requests
// form proxy.Addr to remote.Addr.
package proxy

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/golang/glog"
)

var (
	connid = uint64(0) // Self-increasing ConnectID.
)

// Handler function from proxy to postgresql for rewrite
// request or sql.
type Handler func(get []byte) ([]byte, error)

// Start proxy server needed receive  and proxyHost, all
// the request or database's sql of receive will redirect
// to remoteHost.
func Start(proxyHost, remoteHost string, handler Handler) {
	defer glog.Flush()
	glog.Infof("Proxying from %v to %v\n", proxyHost, remoteHost)

	proxyAddr := getResolvedAddresses(proxyHost)
	remoteAddr := getResolvedAddresses(remoteHost)
	listener := getListener(proxyAddr)

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			glog.Errorf("Failed to accept connection '%s'\n", err)
			continue
		}
		connid++

		p := &Proxy{
			lconn:  conn,
			laddr:  proxyAddr,
			raddr:  remoteAddr,
			erred:  false,
			errsig: make(chan bool),
			prefix: fmt.Sprintf("Connection #%03d ", connid),
			connId: connid,
		}
		go p.service(handler)
	}
}

// ResolvedAddresses of host.
func getResolvedAddresses(host string) *net.TCPAddr {
	addr, err := net.ResolveTCPAddr("tcp", host)
	if err != nil {
		glog.Fatalln("ResolveTCPAddr of host:", err)
	}
	return addr
}

// Listener of a net.TCPAddr.
func getListener(addr *net.TCPAddr) *net.TCPListener {
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		glog.Fatalf("ListenTCP of %s error:%v", addr, err)
	}
	return listener
}

// Proxy - Manages a Proxy connection, piping data between proxy and remote.
type Proxy struct {
	sentBytes     uint64
	receivedBytes uint64
	laddr, raddr  *net.TCPAddr
	lconn, rconn  *net.TCPConn
	erred         bool
	errsig        chan bool
	prefix        string
	connId        uint64
}

// New - Create a new Proxy instance. Takes over local connection passed in,
// and closes it when finished.
func New(conn *net.TCPConn, proxyAddr, remoteAddr *net.TCPAddr, connid uint64) *Proxy {
	return &Proxy{
		lconn:  conn,
		laddr:  proxyAddr,
		raddr:  remoteAddr,
		erred:  false,
		errsig: make(chan bool),
		prefix: fmt.Sprintf("Connection #%03d ", connid),
		connId: connid,
	}
}

// proxy.err
func (p *Proxy) err(s string, err error) {
	if p.erred {
		return
	}
	if err != io.EOF {
		glog.Errorf(p.prefix+s, err)
	}
	p.errsig <- true
	p.erred = true
}

// Proxy.service open connection to remote and service proxying data.
func (p *Proxy) service(handler Handler) {
	defer p.lconn.Close()
	// connect to remote server
	rconn, err := net.DialTCP("tcp", nil, p.raddr)
	if err != nil {
		p.err("Remote connection failed: %s", err)
		return
	}
	p.rconn = rconn
	defer p.rconn.Close()
	// proxying data
	go p.handleIncomingConnection(p.lconn, p.rconn, handler)
	go p.handleResponseConnection(p.rconn, p.lconn)
	// wait for close...
	<-p.errsig
}

// Proxy.handleIncomingConnection
func (p *Proxy) handleIncomingConnection(src, dst *net.TCPConn, customHandler Handler) {
	// directional copy (64k buffer)
	buff := make([]byte, 0xffff)

	for {
		n, err := src.Read(buff)
		if err != nil {
			p.err("Read failed '%s'\n", err)
			return
		}

		b, err := handleQuery(buff[:n], customHandler)
		if err != nil {
			p.err("%s\n", err)
			err = dst.Close()
			if err != nil {
				glog.Errorln(err)
			}
			return
		}

		_, err = dst.Write(b)
		if err != nil {
			p.err("Write failed '%s'\n", err)
			return
		}
	}
}

// Proxy.handleResponseConnection
func (p *Proxy) handleResponseConnection(src, dst *net.TCPConn) {
	// directional copy (64k buffer)
	buff := make([]byte, 0xffff)

	for {
		n, err := src.Read(buff)
		if err != nil {
			p.err("Read failed '%s'\n", err)
			return
		}

		_, err = dst.Write(buff[:n])
		if err != nil {
			p.err("Write failed '%s'\n", err)
			return
		}
	}
}

// query here is somewhat general -- SQL statements are all queries;
// see https://www.postgresql.org/docs/13/protocol-message-formats.html
func handleQuery(input []byte, requestHandler Handler) ([]byte, error) {
	if len(input) > 0 && string(input[0]) == "Q" {
		// TODO: ";", "0" characters should be handled idiomatically

		// first 4 are metadata
		// last character is a NULL / EOF "0"

		// makes an assumption that the last two characters are ";", "0"
		lastTwoBytes := input[len(input)-2:]

		queryStr := input[5 : len(input)-2]

		data, err := requestHandler(queryStr)
		if err != nil {
			log.Fatalln("we failed here", err)
		}

		result := concat(input[0:5], data, lastTwoBytes)

		// update the checksum, because we may have modified
		// the query
		result[4] = byte(len(result) - 1)

		return result, nil
	}

	// no-op if not a Simple Query
	return input, nil
}

// Concat concatenates two slices of strings.
func concat(slices ...[]byte) []byte {
	var totalLen int
	for _, s := range slices {
		totalLen += len(s)
	}
	result := make([]byte, 0, totalLen)
	for _, s := range slices {
		result = append(result, s...)
	}
	return result
}

//// ModifiedBuffer when is local and will call filterCallback function
//func getModifiedBuffer(buffer []byte, filterCallback Handler) (b []byte, err error) {
//	if len(buffer) > 0 && string(buffer[0]) == "Q" {
//		if !filterCallback(buffer) {
//			return nil, errors.New(fmt.Sprintf("Do not meet the rules of the sql statement %s", string(buffer[1:])))
//		}
//	}
//
//	return buffer, nil
//}
