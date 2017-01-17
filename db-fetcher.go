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

	prefix := os.Getenv("S3_FETCHER_DUMP_PREFIX")

	s3Dumps := make([]string, 0)

	for _, key := range resp.Contents {
		if strings.Contains(*key.Key, prefix) {
			s3Dumps = append(s3Dumps, *key.Key)
		}
	}

	latestAvailableDump := s3Dumps[len(s3Dumps)-1]

	files, err := ioutil.ReadDir(os.Args[1])
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	localDumps := make([]string, 0)

	for _, file := range files {
		if strings.HasPrefix(file.Name(), prefix) {
			localDumps = append(localDumps, file.Name())
		}
	}

	currentDump := ""
	if len(localDumps) > 0 {
		currentDump = localDumps[len(localDumps)-1]
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
		os.Exit(66);
	}
}