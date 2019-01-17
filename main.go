package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

var regionFlag = flag.String("region", "eu-west-2", "The AWS region to target.")
var tableNameFlag = flag.String("tableName", "", "The table to download.")
var readCapacityFlag = flag.Int("readCapacity", 5, "Amount of read capacity to consume per second.")

func main() {
	flag.Parse()

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sigs
		log.Printf("Shutting down...")
		cancel()
	}()

	requestsPerSecond := 1
	pageSize := *readCapacityFlag
	if *readCapacityFlag > 100 {
		requestsPerSecond = int(math.Floor((float64(*readCapacityFlag) / 100.0)))
		pageSize = 100
	}
	throttle := time.Tick(time.Second / time.Duration(requestsPerSecond))
	results := make(chan map[string]interface{})

	var wg sync.WaitGroup

	// Write out the results when they arrive.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			for r := range results {
				bytes, err := json.Marshal(r)
				if err != nil {
					log.Printf("failed to marshal map: %v", err)
				}
				fmt.Println(string(bytes))
			}
		}
	}()

	// Start downloading the results.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(results)
		if err := Scan(ctx, *regionFlag, *tableNameFlag, pageSize, throttle, results); err != nil {
			log.Fatal(err)
		}
	}()

	wg.Wait()
}

// Scan the DynamoDB table.
func Scan(ctx context.Context, region, tableName string, pageSize int, throttle <-chan time.Time, target chan<- map[string]interface{}) (err error) {
	conf := &aws.Config{
		Region: aws.String(region),
	}
	sess, err := session.NewSession(conf)
	if err != nil {
		return
	}
	client := dynamodb.New(sess)
	si := &dynamodb.ScanInput{
		ConsistentRead: aws.Bool(false),
		Limit:          aws.Int64(int64(pageSize)),
		TableName:      aws.String(tableName),
	}
	fn := func(page *dynamodb.ScanOutput, lastPage bool) (proceed bool) {
		dataPage := []map[string]interface{}{}
		pageErr := dynamodbattribute.UnmarshalListOfMaps(page.Items, &dataPage)
		if pageErr != nil {
			log.Printf("Error unmarshalling data: %v", pageErr)
			return
		}
		for _, dp := range dataPage {
			dpx := dp
			target <- dpx
		}
		select {
		case <-ctx.Done():
			return
		default:
			break
		}
		<-throttle
		return len(page.LastEvaluatedKey) == 0
	}
	return client.ScanPagesWithContext(ctx, si, fn)
}
