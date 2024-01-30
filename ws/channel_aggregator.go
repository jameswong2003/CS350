package main

import "time"

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
	currentBatch := 0
	// Your code here.
	for {
		currentBatch += 1
		responseCount := 0
		totalTemperature := 0.0

		// Make response channel for response to report to
		responses := make(chan WeatherReport, k)

		for i := 0; i < k; i++ {
			// Go Routine for each weather station
			go func(stationID int, batch int) {
				response := getWeatherData(stationID, batch)
				responses <- response
			}(i, currentBatch)
		}

		// Collect Responses
		for i := 0; i < k; i++ {
			response := <-responses
			if response.Batch == currentBatch {
				totalTemperature += response.Value
				responseCount += 1
			}
		}

		close(responses)

		// Calculate response and send it to the out chan
		if responseCount > 0 {
			averageTemperature := totalTemperature / float64(responseCount)
			report := WeatherReport{Value: averageTemperature, Id: -1, Batch: currentBatch}
			out <- report
		}
		// Delay before the next average period
		time.Sleep(time.Duration(averagePeriod) * time.Second)
	}
}
