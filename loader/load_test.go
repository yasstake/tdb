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
		WriteTrade(writeApi, time.Now(), 1, "sell", float64(i), float64(i))
	}
}

func TestLoad(t *testing.T) {
	//c := influxdb2.NewClient()

	client := OpenClient()
	writeApi := NewWriteApi(client)

	p := influxdb2.NewPoint("order",
		map[string]string{"side": "buy", "p": "10.5"},
		map[string]interface{}{"price": 24.5, "vol": 45},
		time.Now())

	writeApi.WritePoint(p)
	client.Close()
}

func TestQuery(t *testing.T) {
	client := OpenClient()
	defer client.Close()
	q := NewQueryApi(client)

	result, err := q.Query(context.Background(), `from(bucket:"btc") |> range(start: -2d)|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")`)

	if err != nil {
		fmt.Println(err)
		return
	}

	log.Printf("loading...\n")

	for result.Next() {
		value := result.Record().Values()
		log.Printf("value: %f %f\n", value["price"], value["size"])
	}

}

func TestCompareTimeStamp(t *testing.T) {
	t1 := float64(1629935999.664)
	t2 := float64(1629935999.664)
	t3 := float64(1629935999.66)

	time1 := SecToTime(t1)
	time2 := SecToTime(t2)
	time3 := SecToTime(t3)

	if time1 != time2 {
		t.Errorf("unmach %s, %s", time1, time2)
	}

	if time2 == time3 {
		t.Errorf("unmach %s, %s", time2, time3)
	}

	if !time1.Equal(time2) {
		t.Errorf("unmach %s, %s", time1, time2)
	}
}

func TestLoadCsv(t *testing.T) {
	// file := "../DATA/BTCUSD2021-05-14.csv.gz"
	file := "../DATA/BTCUSD2021-08-25.csv.gz"

	LoadCsv(file)
}

func TestTimeDiff(t *testing.T) {
	t1 := SecToTime(float64(1629935999.664))
	t2 := SecToTime(float64(1629935999.664001))

	diff := t2.Sub(t1)

	log.Println("diff", diff.Nanoseconds())
}

func TestParseTimeStamp(t *testing.T) {
	var sec, nsec int
	s := "1629935999.664"

	fmt.Sscanf(s, "%d.%d", &sec, &nsec)

	log.Println(sec)
	log.Println(nsec)
}
