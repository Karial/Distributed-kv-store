package main

import (
	"Trying_to_implement_atomic_KV/src"
	"flag"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

func main() {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100
	rand.Seed(time.Now().Unix())

	isReplica := flag.Bool("replica", false, "Do we start replica")
	isCoordinator := flag.Bool("coordinator", false, "Do we start coordinator")
	port := flag.Int("port", 3000, "Port for the server to listen on")
	db := flag.String("db", "", "Path to leveldb")
	replicas := flag.String("replicas", "", "Replicas to use for storage, comma separated")
	guid := flag.String("guid", "", "Global unique identifier for coordinator. "+
		"Should be unique between all coordinators.")
	verbose := flag.Bool("v", false, "Verbose output")
	flag.Parse()

	if !*verbose {
		log.SetOutput(ioutil.Discard)
	} else {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	}

	if !*isReplica && !*isCoordinator {
		panic("You should choose whether replica or coordinator")
	}

	if *isReplica {
		src.RunReplica(*port, *db)
	} else {
		src.RunCoordinator(strings.Split(*replicas, ","), *guid, *port)
	}
}
