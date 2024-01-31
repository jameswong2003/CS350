package main

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func init() {
	rpc.HandleHTTP()
}

var listenerPointers = make([]*net.Listener, 0)
var socketNames = make([]string, 0)

// Test the channel-based approach for aggregating weather data.
func TestChannelAggregator(t *testing.T) {
	runTests(t, channelAggregator)
}

// Test the mutex-based approach for aggregating weather data.
func TestMutexAggregator(t *testing.T) {
	runTests(t, mutexAggregator)
}

func createWeatherStations(t *testing.T, k int, averagePeriod float64, testName string) {
	stations := make([]WeatherStation, k)
	for i := 0; i < k; i++ {
		stations[i] = WeatherStation{i, averagePeriod, testName}
		sockAddr := fmt.Sprintf("/tmp/%s_%d.sock", strings.Replace(testName, "/", "", -1), i)

		rpc.Register(&stations[i])
		//l, e := net.Listen("tcp", ":1234")
		os.Remove(sockAddr)

		l, e := net.Listen("unix", sockAddr)
		if e != nil {
			log.Fatal("listen error:", e)
		}
		go http.Serve(l, nil)

		// Register a cleanup element for the current station.
		listenerPointers = append(listenerPointers, &l)
		socketNames = append(socketNames, sockAddr)
	}
}

// A helper function that executes a set of tests on a given aggregator function.
func runTests(
	t *testing.T,
	aggregatorFunc func(int, float64, func(int, int) WeatherReport, chan WeatherReport, chan struct{}),
) {
	testCases := []struct {
		name  string
		setup func()
	}{
		{"TestK1", func() { AveragePeriod = 0.8; K = 1; QuitEarly = false }},
		{"TestK10", func() { AveragePeriod = 0.8; K = 10; QuitEarly = false }},
		{"TestK100", func() { AveragePeriod = 0.8; K = 100; QuitEarly = false }},
		{"TestLongAveragePeriod", func() { AveragePeriod = 5.0; K = 100; QuitEarly = false }},
		{"TestQuit", func() { AveragePeriod = 0.8; K = 100; QuitEarly = true }},
	}

	for _, tc := range testCases {
		tc.setup()
		createWeatherStations(t, K, AveragePeriod, t.Name()+tc.name)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup()
			testAll(t, aggregatorFunc)
		})
	}
}

// Run a series of tests on a given implementation of the weather data aggregation logic.
//
// `K` is the number of weather stations.
// `AveragePeriod` defines the average period between weather data requests.
// `QuitEarly` determines if the tester should inform the aggregator to quit early.
func testAll(
	t *testing.T,
	aggregatorFunc func(int, float64, func(int, int) WeatherReport, chan WeatherReport, chan struct{}),
) {
	out := make(chan WeatherReport)
	quit := make(chan struct{})

	backChannelValues := make(chan internalWeatherCallReport, K*10)
	backChannelRPCReceipts := make(chan weatherCallReceipt, K*10)

	getWeatherData := NewGetWeatherDataClosure(AveragePeriod, backChannelValues, backChannelRPCReceipts, t.Name())

	go aggregatorFunc(K, AveragePeriod, getWeatherData, out, quit)

	testingTime := 6.
	quitTime := testingTime * 2
	if QuitEarly {
		quitTime = testingTime / 2
	}

	dieAtTimer := time.NewTimer(time.Duration((testingTime+0.1)*1000) * time.Millisecond)
	quitTimer := time.NewTimer(time.Duration((quitTime+0.1)*1000) * time.Millisecond)
	batchCount := 0
	expectedCount := int(testingTime / AveragePeriod)
	now := time.Now()
	done := false

	for !done {
		select {
		case studentAnswer := <-out:
			fmt.Println("Got answer for Batch", studentAnswer.Batch)
			values := make(map[int]bool)
			toBeRecycledReceipts := make([]weatherCallReceipt, K*2)

			for len(backChannelRPCReceipts) > 0 {
				receipt := <-backChannelRPCReceipts
				if receipt.batch == studentAnswer.Batch {
					if values[receipt.id] == true {
						t.Fatal("ERROR: duplicate RPC for Id", receipt.id)

					} else {
						values[receipt.id] = true
					}
				} else if receipt.batch > studentAnswer.Batch {
					// Place the receipt back in the channel to be recycled if it is part of a future Batch.
					toBeRecycledReceipts = append(toBeRecycledReceipts, receipt)
				}
			}
			for _, receipt := range toBeRecycledReceipts {
				backChannelRPCReceipts <- receipt
			}

			if len(values) != K {
				t.Fatal("ERROR: expected", K, "RPCs to be made, but got", len(values), "\n\n",
					"RPCs were made for the following ids:",
					func() string {
						s := ""
						for id := range values {
							s += strconv.Itoa(id) + " "
						}
						return s
					}())
			}

			total := 0.0
			count := 0

			toBeRecycledValues := make([]internalWeatherCallReport, K*2)
			for len(backChannelValues) > 0 {
				report := <-backChannelValues

				if report.batch == studentAnswer.Batch {
					// Only include the report if it was requested as part of this Batch.
					total += report.value
					count++
				} else if report.batch > studentAnswer.Batch {
					// Place the report back in the channel to be recycled if it is part of a future Batch.
					toBeRecycledValues = append(toBeRecycledValues, report)
				}
			}
			for _, report := range toBeRecycledValues {
				backChannelValues <- report
			}

			// Print the average and the student's answer.
			fmt.Println("True answer:", total/float64(count))
			fmt.Println("Your answer:", studentAnswer.Value)
			fmt.Println("Samples seen by the grader:", count)
			fmt.Println("Time elapsed:", time.Since(now).Seconds())

			// Confirm that the student's answer is within 0.05 of the true answer
			// and that the time elapsed is within 0.1 of AveragePeriod.
			if studentAnswer.Value < total/float64(count)-0.08 || studentAnswer.Value > total/float64(count)+0.08 {
				t.Fatal("ERROR: Your answer is not within 0.08 of the true answer")
			}
			if time.Since(now).Seconds() < AveragePeriod-0.08 || time.Since(now).Seconds() > AveragePeriod+0.08 {
				t.Fatal("ERROR: Time elapsed is not within 0.08 of AveragePeriod")
			}

			// Confirm that the student's NaN answer matches ours.
			if math.IsNaN(studentAnswer.Value) && !math.IsNaN(total/float64(count)) {
				t.Fatal("ERROR: Your answer is NaN, but ours isn't")
			}
			if !math.IsNaN(studentAnswer.Value) && math.IsNaN(total/float64(count)) {
				t.Fatal("ERROR: Your answer is not NaN, but ours is")
			}

			batchCount++
			fmt.Println("####")
			now = time.Now()

		case <-quitTimer.C:
			go func() { quit <- struct{}{} }()

			select {
			case <-out:
				t.Fatal("ERROR: received answer after quit signal was sent")
			case <-time.After(time.Duration(AveragePeriod*1000*2) * time.Millisecond):
				return
			}

		case <-dieAtTimer.C:
			done = true
			if batchCount != expectedCount {
				t.Fatal("ERROR: expected", expectedCount, "batches, but got", batchCount)
			}
		}
	}
}

// cleanup method to run last

func TestCleanup(t *testing.T) {
	// wait 1 second to wait for all the aggregators to block/exit
	time.Sleep(time.Second)

	// call close on all the listeners
	for _, listener := range listenerPointers {
		(*listener).Close()
	}

	// delete all the sockets
	for _, socket := range socketNames {
		os.Remove(socket)
	}
}
