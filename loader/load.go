package main

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/influxdata/influxdb-client-go/api"
)

var INFLUXDB_KEY string

func init() {
	INFLUXDB_KEY = os.Getenv("INFLUXDB_KEY")
}

func SecToTime(t float64) time.Time {
	return time.Unix(0, int64(t*1000_000_000))
}

func OpenClient() influxdb2.Client {
	// Store the URL of your InfluxDB instance
	url := "http://localhost:8086"

	//client := influxdb2.NewClient(url, token)
	client := influxdb2.NewClientWithOptions(url, INFLUXDB_KEY, influxdb2.DefaultOptions().SetBatchSize(5000))

	return client
}

func NewWriteApi(client influxdb2.Client) api.WriteApi {
	bucket := "btc"
	org := "bb"

	//writeAPI := client.WriteAPIBlocking(org, bucket)
	writeAPI := client.WriteAPI(org, bucket)

	return writeAPI
}

func NewQueryApi(client influxdb2.Client) api.QueryApi {
	api := client.QueryAPI("bb")

	return api
}

func WriteTrade(w api.WriteAPI, time_stamp time.Time, side string, price float64, size float64) {

	p := influxdb2.NewPoint("order",
		map[string]string{"tran": side},
		map[string]interface{}{"price": price, "size": size},
		time_stamp)

	w.WritePoint(p)
}

func LoadCsv(file string) {
	f, err := os.Open(file)

	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	compress := strings.HasSuffix(file, ".gz")
	var r *csv.Reader
	if compress {
		gzipfile, _ := gzip.NewReader(f)
		r = csv.NewReader(gzipfile)
	} else {
		r = csv.NewReader(f)
	}
	r.FieldsPerRecord = -1 // ignore feild number varies

	client := OpenClient()
	defer client.Close()
	writer := NewWriteApi(client)
	defer writer.Flush()

	// Skip One Line
	_, _ = r.Read()

	for {
		row, err := r.Read()
		if err == io.EOF {
			fmt.Println("[PROCESS DONE]")
			break
		}
		if err != nil {
			fmt.Println("[FILE READ ERROR]", err)
			break
		}

		// 0                 1       2     3     4
		// time             ,symbol, side, size, price,
		// 1621009144.802278,BTCUSD,Buy,432,51325.0,PlusTick,28eb5240-d904-59e7-a40f-ba420de466c8,841695.0,432,0.00841695

		var time_stamp time.Time
		var action string
		var size float64
		var price float64

		// records := int(0)

		for i, v := range row {
			if i == 0 {
				r, err := strconv.ParseFloat(v, 64)
				if err != nil {
					break
				}
				time_stamp = SecToTime(r)
			} else if i == 1 {
				// ignore symbol (BTCUSD)
			} else if i == 2 {
				if v == "Buy" {
					// fmt.Println(v)
				} else if v == "Sell" {
					// fmt.Println(v)
				} else {
					break
				}
				action = v
			} else if i == 3 {
				s, err := strconv.ParseFloat(v, 64)
				if err != nil {
					break
				}
				size = s
			} else if i == 4 {
				p, err := strconv.ParseFloat(v, 64)
				if err != nil {
					break
				}
				price = p
			} else {
				break
			}
		}

		WriteTrade(writer, time_stamp, action, price, size)

		fmt.Print("*")
	}
}
