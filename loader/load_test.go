package main

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go"
)

func TestSecToTime(t *testing.T) {
	tm := SecToTime(1621036798.423747)
	fmt.Println(tm)
	log.Println(tm)
}

func TestWriteTrade(t *testing.T) {
	client := OpenClient()
	writeApi := NewWriteApi(client)

	for i := 1; i < 10000; i += 1 {
		WriteTrade(writeApi, time.Now(), "sell", float64(i), float64(i))
	}
}

func TestLoad(t *testing.T) {
	//c := influxdb2.NewClient()

	client := OpenClient()
	writeApi := NewWriteApi(client)

	p := influxdb2.NewPoint("order",
		map[string]string{"side": "buy"},
		map[string]interface{}{"price": 24.5, "vol": 45},
		time.Now())

	writeApi.WritePoint(p)
	client.Close()
}

func TestQuery(t *testing.T) {
	client := OpenClient()
	defer client.Close()
	q := NewQueryApi(client)

	result, err := q.Query(context.Background(), `from(bucket:"btc") |> range(start: -2d)`)

	if err != nil {
		fmt.Println(err)
		return
	}

	for result.Next() {
		fmt.Printf("value: %s\n", result.Record().Values())
	}

}

func TestLoadCsv(t *testing.T) {
	file := "../DATA/BTCUSD2021-05-14.csv.gz"

	LoadCsv(file)
}
