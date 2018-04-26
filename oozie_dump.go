package main

import (
	"time"
	"os"
	"net/http"
	"fmt"
	"encoding/json"
	"strings"
	"github.com/aws/aws-sdk-go/private/protocol/rest"
	"log"
	"path"
	"io/ioutil"
	"regexp"
	"encoding/csv"
)

const TIME_LAYOUT = rest.RFC822
const APPNAME_REG = "^aws-reconciler-production-([0-9]{12})-([0-9]{4})-([0-9]{2})$"
const TIME_FILENAME = "2006-01-02T15-04-05Z"
const MARKER_FORMAT = time.RFC3339

type CustomTime struct {
	time.Time
}

func (ct *CustomTime) UnmarshalJSON(b []byte) (err error) {
	s := strings.Trim(string(b), "\"")
	if s == "null" {
		ct.Time = time.Time{}
		return
	}
	ct.Time, err = time.Parse(TIME_LAYOUT, s)
	return
}

type Workflow struct {
	Status string `json:"status"`
	CreatedTime CustomTime `json:"createdTime"`
	Id string `json:"id"`
	AppName string `json:"appName"`
	User string `json:"user"`
	LastModTime CustomTime `json:"lastModTime"`
	EndTime CustomTime `json:"endTime"`
}

type Jobs struct{
	Total int `json:"total"`
	Workflows []Workflow `json:"workflows"`
	Len int `json:"len"`
	Offset int `json:"offset"`
}

func loadFromUrl()(Jobs, error){
	result := Jobs{}

	if os.Getenv("OOZIE_URL") == "" {
		return result, fmt.Errorf("the environment OOZIE_URL is not set")
	}

	fullUrl := os.Getenv("OOZIE_URL") + "v1/jobs"

	resp, err := http.Get(fullUrl)

	if err != nil {
		return result, fmt.Errorf("failed to open oozie api:%v", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return result, fmt.Errorf("request returned status:%d %s", resp.StatusCode, resp.Status)
	}

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&result)

	if err != nil {
		return result, fmt.Errorf("failed to parse api response:%v", err)
	}

	return result, nil
}

func loadFromFile(fileName string)(Jobs,error){
	fh, err := os.Open(fileName)
	result := Jobs{}

	if err != nil {
		return result, fmt.Errorf("failed to open file:%v", err)
	}

	defer fh.Close()

	decoder := json.NewDecoder(fh)
	err = decoder.Decode(&result)

	if err != nil {
		return result, fmt.Errorf("failed to parse file contents:%v", err)
	}

	return result, nil
}

func readMarker(outputDir string)(time.Time,error){
	markerPath := path.Join(outputDir, "_marker")

	b,err := ioutil.ReadFile(markerPath)
	result := time.Date(
		2000,
		1,
		1,
		0,
		0,
		0,
		0,
		time.UTC)

	if err != nil && os.IsNotExist(err) {
		return result, nil
	} else if err != nil {
		return result, err
	}

	return time.Parse(MARKER_FORMAT, string(b))
}

func writeUpdate(jobs Jobs, outputDir string, markerTime time.Time) error{
	re, err := regexp.Compile(APPNAME_REG)

	if err != nil {
		return fmt.Errorf("failed to compile regex:%v", err)
	}

	lastMod := markerTime

	outputPath := path.Join(outputDir, time.Now().In(time.UTC).Format(TIME_FILENAME) + ".csv")

	fw, err := os.Create(outputPath)

	if err != nil {
		return fmt.Errorf("cannot create output file:%v", err)
	}

	defer fw.Close()

	writer := csv.NewWriter(fw)

	defer writer.Flush()

	writer.Write([]string{
		"id",
		"appName",
		"master",
		"year",
		"month",
		"status",
		"createdTime",
		"lastModTime",
		"endTime"})

	for _, workflow := range jobs.Workflows {
		if re.MatchString(workflow.AppName) && markerTime.Before(workflow.LastModTime.Time) {
			if lastMod.Before(workflow.LastModTime.Time) {
				lastMod = workflow.LastModTime.Time
			}

			parts := re.FindStringSubmatch(workflow.AppName)

			writer.Write([]string{
				workflow.Id,
				workflow.AppName,
				parts[1],
				parts[2],
				parts[3],
				workflow.Status,
				workflow.CreatedTime.Format(time.RFC3339),
				workflow.LastModTime.Format(time.RFC3339),
				workflow.EndTime.Format(time.RFC3339)})

			log.Printf("Write workflow:%s", workflow.AppName)
		}
	}

	markerPath := path.Join(outputDir, "_marker")
	markerBytes := []byte(lastMod.Format(MARKER_FORMAT))
	err = ioutil.WriteFile(markerPath, markerBytes, 0600)

	if err != nil {
		return fmt.Errorf("failed to write marker:%v", err)
	}

	return nil
}

func main() {
	var err error
	var jobs Jobs
	var outputDir string

	if len(os.Args) == 3 {
		jobs, err = loadFromFile(os.Args[1])
		outputDir = os.Args[2]
	} else if len(os.Args) == 2 {
		jobs, err = loadFromUrl()
		outputDir = os.Args[1]
	} else {
		fmt.Fprintf(os.Stderr, "usage:%s [inputFile] outputDir\n", os.Args[0])
		os.Exit(1)
	}

	if err != nil {
		panic(err)
	}

	stat, err := os.Stat(outputDir)

	if err != nil && os.IsNotExist(err) {
		err = os.Mkdir(outputDir, 0700)

		if err != nil {
			log.Panicf("cannot create output dir:%v",err)
		}
	} else if err != nil {
		log.Panic(err)
	} else if !stat.IsDir() {
		log.Panic("output dir is not a directory")
	}

	marker, err := readMarker(outputDir)

	if err != nil {
		log.Panicf("failed to read marker file:%v", err)
	}

	log.Printf("Writing workflows from:%s", marker.Format(time.RFC3339))

	err = writeUpdate(jobs, outputDir, marker)

	if err != nil {
		log.Panic(err)
	}
}

