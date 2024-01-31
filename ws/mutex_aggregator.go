package main

import (
	"fmt"
	"sync"
	"time"
)

// Mutex-based aggregator that reports the global average temperature periodically
//
// Report the averagage temperature across all `k` weatherstations every `averagePeriod`
// seconds by sending a `WeatherReport` struct to the `out` channel. The aggregator should
// terminate upon receiving a singnal on the `quit` channel.
//
// Note! To receive credit, mutexAggregator must implement a mutex based solution.

type Container struct {
	mu               sync.Mutex
	totalTemperature float64
	responseCount    int
	currentBatch     int
}

func (c *Container) record(temperature float64, batch int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if batch == c.currentBatch {
		c.totalTemperature += temperature
		c.responseCount += 1
	}
}

func mutexAggregator(
	k int,
	averagePeriod float64,
	getWeatherData func(int, int) WeatherReport,
	out chan WeatherReport,
	quit chan struct{},
) {
	ticker := time.NewTicker(time.Duration(averagePeriod*1000) * time.Millisecond)

	c := Container{totalTemperature: 0.0, responseCount: 0, currentBatch: 0}

	for i := 0; i < k; i++ {
		go func(stationID int, batch int) {
			response := getWeatherData(stationID, batch)
			c.record(response.Value, batch)
		}(i, c.currentBatch)
	}

	for {
		select {
		case <-quit:
			return
		case <-ticker.C:
			c.mu.Lock()
			averageTemperature := c.totalTemperature / float64(c.responseCount)
			report := WeatherReport{Value: averageTemperature, Batch: c.currentBatch}
			fmt.Println(c.totalTemperature, c.responseCount)
			out <- report

			c.totalTemperature = 0
			c.responseCount = 0
			c.currentBatch += 1
			c.mu.Unlock()

			for i := 0; i < k; i++ {
				go func(stationID int, batch int) {
					response := getWeatherData(stationID, batch)
					c.record(response.Value, batch)
				}(i, c.currentBatch)
			}

		}
	}
}
