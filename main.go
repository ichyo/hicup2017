package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
)

func handler(writer http.ResponseWriter, request *http.Request) {
	writer.Write([]byte("hello world"))
}

func main() {
	port := flag.Int("port", 8080, "port number")
	flag.Parse()

	http.HandleFunc("/", handler)

	log.Println("Start running on", *port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}
