package main

import (
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
}

func (c *Container) record(temperature float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.totalTemperature += temperature
	c.responseCount += 1
}

func mutexAggregator(
	k int,
	averagePeriod float64,
	getWeatherData func(int, int) WeatherReport,
	out chan WeatherReport,
	quit chan struct{},
) {
	ticker := time.NewTicker(time.Duration(averagePeriod*1000) * time.Millisecond)
	currentBatch := 0

	c := Container{totalTemperature: 0.0, responseCount: 0}

	for i := 0; i < k; i++ {
		go func(stationID int, batch int) {
			response := getWeatherData(stationID, batch)
			c.record(response.Value)
		}(i, currentBatch)
	}

	for {
		select {
		case <-quit:
			return
		case <-ticker.C:
			averageTemperature := c.totalTemperature / float64(c.responseCount)
			report := WeatherReport{Value: averageTemperature, Batch: currentBatch}
			out <- report

			c.mu.Lock()
			c.totalTemperature = 0
			c.responseCount = 0
			c.mu.Unlock()

			currentBatch += 1

			for i := 0; i < k; i++ {
				go func(stationID int, batch int) {
					response := getWeatherData(stationID, batch)
					c.record(response.Value)
				}(i, currentBatch)
			}

		}
	}
}
