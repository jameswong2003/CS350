package main

import (
	"math/rand"
	"time"
)

type Args struct {
	Batch int
}

type WeatherStation struct {
	id            int
	averagePeriod float64
	testName      string
}

// RPC handler that simulates a weather station
//
// Returns a WeatherReport with a temperature Value after a randomized delay,
// with a small chance of hanging indefinitely.
func (t *WeatherStation) GetWeatherRPCHandler(args *Args, reply *WeatherReport) error {
	// There is a 1/10 chance that the weather station does not respond at all.
	random := rand.Intn(10)
	if random == 6 {
		select {}
	}

	before := rand.Intn(2)
	margin := 0.15

	var delay time.Duration
	delay = time.Duration(rand.Float64() * (t.averagePeriod - margin) * float64(time.Second.Nanoseconds()))

	if before == 1 {
		// We're good, we've already delayed enough.
	} else {
		delay += time.Duration((t.averagePeriod + (2 * margin)) * float64(time.Second.Nanoseconds()))
	}

	time.Sleep(delay)

	value := float64(rand.Intn(100))
	*reply = WeatherReport{value, t.id, args.Batch}
	return nil
}
