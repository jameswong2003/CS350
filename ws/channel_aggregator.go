package main

import (
	"math"
	"time"
)

// Channel-based aggregator that reports the global average temperature periodically
//
// Report the averagage temperature across all `k` weatherstations every `averagePeriod`
// seconds by sending a `WeatherReport` struct to the `out` channel. The aggregator should
// terminate upon receiving a singnal on the `quit` channel.
//
// Note! To receive credit, channelAggregator must not use mutexes.
func channelAggregator(
	k int,
	averagePeriod float64,
	getWeatherData func(int, int) WeatherReport,
	out chan WeatherReport,
	quit chan struct{},
) {
	ticker := time.NewTicker(time.Duration(averagePeriod*1000) * time.Millisecond)
	select {
	case <-quit:
		return
	case <-ticker.C:
		currentBatch := 0
		totalTemperature := 0.0
		responseCount := 0

		responses := make(chan WeatherReport)

		for i := 0; i < k; i++ {
			go func(stationID int, batch int) {
				response := getWeatherData(stationID, batch)
				responses <- response
			}(i, currentBatch)
		}

		for i := 0; i < k; i++ {
			response := <-responses
			if response.Batch == currentBatch {
				totalTemperature += response.Value
				responseCount += 1
			}
		}
		close(responses)

		if responseCount > 0 {
			averageTemperature := totalTemperature / float64(responseCount)
			report := WeatherReport{Value: averageTemperature, Id: -1, Batch: currentBatch}
			out <- report
		} else {
			report := WeatherReport{Value: math.NaN(), Batch: currentBatch}
			out <- report
		}

		currentBatch += 1
	}

}
