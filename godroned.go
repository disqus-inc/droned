package main

import (
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"time"

	"github.com/getsentry/raven-go"
)

var sentryDSN string = "<SENTRY DSN GOES HERE>"

func main() {

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Go Drone")
	})

	http.HandleFunc("/query/", hiveQuery)

	//go testQueryEndpoint()
	log.Fatal(http.ListenAndServe(":5000", nil))

}

func logSentry(err error, r *http.Request) {
	sentryClient, serr := raven.NewClient(sentryDSN, nil)
	if serr != nil {
		log.Printf("Sentry encoutered error: %s", serr)
	}

	trace := raven.NewStacktrace(0, 2, nil)
	packet := raven.NewPacket(err.Error(), raven.NewException(err, trace), raven.NewHttp(r))
	eventID, ch := sentryClient.Capture(packet, nil)
	if cerr := <-ch; cerr != nil {
		log.Printf("Sentry Channel encountered error: %s", cerr)
	}
	message := fmt.Sprintf("Captured error with id %s: %q", eventID, err)
	log.Println(message)
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
		out, err = runHive(query, r)
	}
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "%s", err.Error())
	} else {
		fmt.Fprintf(w, "%s", out)
	}
}

func crc32String(s string) string {
	return string(crc32.ChecksumIEEE([]byte(s)))
}

func runHive(query string, r *http.Request) (result []byte, err error) {
	log.Printf("Hive Query: %s\n", query)
	cmd := exec.Command("hive", "-e", query)
	result, err = cmd.Output()
	if err != nil {
		logSentry(err, r)
	}
	return
}

func runHiveWithStderr(query string, r *http.Request) (result []byte, err error) {
	log.Printf("Hive Query: %s\n", query)
	cmd := exec.Command("hive", "-e", query)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		logSentry(err, r)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		logSentry(err, r)
	}
	if err := cmd.Start(); err != nil {
		logSentry(err, r)
	}
	result, err = ioutil.ReadAll(stdout)
	if err != nil {
		logSentry(err, r)
	}

	dstName := crc32String(query)
	dst, err := os.Create(dstName)
	if err != nil {
		logSentry(err, r)
	}
	defer dst.Close()

	_, err = io.Copy(dst, stderr)
	if err != nil {
		logSentry(err, r)
	}

	if err := cmd.Wait(); err != nil {
		logSentry(err, r)
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
