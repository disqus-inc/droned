package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os/exec"
	"time"

	"github.com/getsentry/raven-go"
)

var sentryDSN string = "<SENTRY DSN HERE>"

func main() {

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Go Drone")
	})

	http.HandleFunc("/query/", hiveQuery)

	//go testQueryEndpoint()
	log.Fatal(http.ListenAndServe(":5000", nil))

}

func hiveQuery(w http.ResponseWriter, r *http.Request) {

	err := r.ParseForm()
	if err != nil {
		log.Fatal(err)
	}

	query := r.Form["query"][0]
	out, err := runHive(query, r)
	if err != nil {
		log.Println("Hive failed, retrying in 30 seconds")
		time.Sleep(time.Duration(30) * time.Second)
		out, _ = runHive(query, r)
	}
	fmt.Fprintf(w, "%s", out)
}

func runHive(query string, r *http.Request) (result []byte, err error) {
	log.Printf("Hive Query: %s\n", query)
	result, err = exec.Command("hive", "-e", query).Output()
	if err != nil {
		sentryClient, serr := raven.NewClient(sentryDSN, nil)
		if serr != nil {
			log.Printf('Sentry encoutered error: %s', serr)
		}

		trace := raven.NewStacktrace(0, 2, nil)
		packet := raven.NewPacket(err.Error(), raven.NewException(err, trace), raven.NewHttp(r))
		eventID, ch := sentryClient.Capture(packet, nil)
		if cerr := <-ch; cerr != nil {
			log.Printf('Sentry Channel encountered error: %s', cerr)
		}
		message := fmt.Sprintf("Captured error with id %s: %q", eventID, err)
		log.Println(message)
	}
	return
}

func testQueryEndpoint() {
	resp, err := http.PostForm("http://127.0.0.1:5000/query/", url.Values{"query": {"show tables"}})
	if err != nil {
		log.Fatal()
	}

	resp.Body.Close()
}
