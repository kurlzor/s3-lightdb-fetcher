package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"io/ioutil"
	"strings"
	"os"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"time"
	"log"
)

func main() {
	creds := credentials.NewEnvCredentials()
	_, err := creds.Get()
	if err != nil {
		fmt.Printf("bad credentials: %s", err)
	}

	bucket := os.Getenv("S3_FETCHER_BUCKET")
	cfg := aws.NewConfig().WithRegion(os.Getenv("S3_FETCHER_BUCKET_REGION")).WithCredentials(creds)
	svc := s3.New(session.New(), cfg)

	params := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	}

	if err != nil {
		fmt.Printf("%s", err)
	}

	resp, err := svc.ListObjects(params)

	if err != nil {
		fmt.Printf("%s", err)
	}

	dumps := make([]string, 0)

	for _, key := range resp.Contents {
		if strings.Contains(*key.Key, os.Getenv("S3_FETCHER_DUMP_PREFIX")) {
			dumps = append(dumps, *key.Key)
		}
	}

	latestAvailableDump := dumps[len(dumps)-1]

	files, err := ioutil.ReadDir(os.Args[1])
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	currentDump := ""
	if len(files) > 0 {
		currentDump = files[len(files)-1].Name()
		fmt.Fprintf(os.Stderr, "Current dump: %s\n", currentDump);
	} else {
		currentDump = ""
		fmt.Fprintln(os.Stderr, "No dump found");
	}

	fmt.Fprintf(os.Stderr, "Latest dump: %s\n", latestAvailableDump);

	if strings.Compare(latestAvailableDump, currentDump) == 1 {
		fmt.Fprintf(os.Stderr, "Latest dump: %s\n", latestAvailableDump)

		s3dl := s3manager.NewDownloaderWithClient(svc)

		start := time.Now()

		file, err := os.Create(os.Args[1] + "/" + latestAvailableDump)

		n, err := s3dl.Download(file, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(latestAvailableDump),
		})

		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		fmt.Fprintf(os.Stderr, "wrote %v bytes to stdout in %v\n", n, time.Now().Sub(start))

	} else {
		fmt.Fprintln(os.Stderr, "You already have the latest dump")
	}
}