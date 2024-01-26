package main

import (
	"fmt"
	"log"
	"net/rpc"
	"strings"
	"time"
)

// A WeatherReport holds the data for a single weather report
// returned by a weather station.
type WeatherReport struct {
	Value float64 // The measured temperature Value
	Id    int     // Identifier for the weather station
	Batch int     // The Batch number for this report
}

// internalWeatherCallReport is used by the tester to track
// the timings and details of weather data responses.
type internalWeatherCallReport struct {
	requestedAtTime time.Time
	returnedAtTime  time.Time
	value           float64
	id              int
	batch           int
	delay           int64
}

// weatherCallReceipt is used by the tester to track
// the receipt of weather data requests.
type weatherCallReceipt struct {
	id    int
	batch int
}

var AveragePeriod float64
var QuitEarly bool
var K int

// A factory function that returns a closure for obtaining weather data
//
// Returns a closure invoked by the student solutions in `aggregator.go`
// and the tester. Ensures that different testing runs are isolated from each other.
func NewGetWeatherDataClosure(
	averagePeriod float64,
	backChannelValues chan internalWeatherCallReport,
	backChannelRPCReceipts chan weatherCallReceipt,
	testName string,
) (getWeatherData func(int, int) WeatherReport) {
	// getWeatherData makes a RPC call to the weather station and returns a
	// WeatherReport with a delay and a small chance of hanging indefinitely.
	getWeatherData = func(id int, batch int) WeatherReport {
		requestedAtTime := time.Now()
		backChannelRPCReceipts <- weatherCallReceipt{id, batch}

		startTime := time.Now()

		// Establish an RPC client connection using a UNIX socket.
		client, err := rpc.DialHTTP("unix", fmt.Sprintf("/tmp/%s_%d.sock", strings.Replace(testName, "/", "", -1), id))
		if err != nil {
			log.Fatal("dialing:", err)
		}

		// Perform a synchronous RPC call to the WeatherStation's GetWeatherRPCHandler method.
		args := Args{batch}
		var reply WeatherReport
		err = client.Call("WeatherStation.GetWeatherRPCHandler", args, &reply)
		if err != nil {
			log.Fatal("getWeather error:", err)
		}

		returnedAtTime := time.Now()

		delay := returnedAtTime.Sub(startTime)

		if float64(delay.Microseconds()) < averagePeriod*float64(time.Second.Microseconds()) {
			backChannelValues <- internalWeatherCallReport{
				requestedAtTime,
				returnedAtTime,
				reply.Value,
				id,
				batch,
				delay.Nanoseconds(),
			}
		}

		return WeatherReport{reply.Value, id, batch}
	}
	return // Naked return
}
