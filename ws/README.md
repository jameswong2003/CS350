# Exercise 1: Weather Stations

**Due: Fri, Feb 2**

Please make sure to regularly commit and push your work to gitlab so that we can see your progress.  
_Final submission will be on gradescope_ – this will be the **only** source of truth for grading.

There will be no late submission and no extensions, so make sure to submit on time.

# Introduction

There are `k` weather stations around the world. Your task is to compute the current average temperature across these stations every `averagePeriod` seconds. We will provide a function that queries the temperature data at a given weather station and returns the current temperature. The function makes RPC calls across the network to the weather stations.

However, the weather station may take some time to respond or may not respond at all. Compute the average temperature for the weather stations that do respond, ensuring the entire calculation is completed within `averagePeriod` seconds. Your implementation should also gracefully handle shutdown requests from the `quit` channel.

<u>**Late responses should be ignored instead of included in the next batch.**</u>

If a batch contains zero observations, return NAN (this is the default behavior of floating point division by zero in Go).

You will be implementing **two** distinct solutions to this problem.

1. **Channel-based solution:** Your first implementation should exclusively utilize channels. In this approach, the use of mutexes or locks is not permitted.
2. **Mutex-based solution:** For your second implementation, you should modify your approach to instead rely on mutexes for managing concurrency.

## API

Your code will make calls to the getWeatherData function provided as an argument to your aggregator, which takes a weather station ID and a batch ID and returns a WeatherReport struct. Here are the signatures:

```go
type WeatherReport struct {
   Value float64
   Id    int
   Batch int
}

func getWeatherData(id int, batch int) WeatherReport {}
```

Your code should call GetWeatherData once for each weather station 0-k. For each batch of `averagePeriod` seconds of observations, increment the `batch` parameter. For example, if `averagePeriod` is 5 seconds, then the first batch will be 0, and then 5 seconds later, you will request a new batch with `batch` set to 1 and use those observations to compute the average temperature for batch 1.

The `value` parameter is the temperature reading from the weather station.

Every `averagePeriod` seconds, your code should send a `WeatherReport` to the `out` channel. The `WeatherReport` should have the average temperature and the batch number. (The `id` field can be set to -1 or left blank.)

Your code must terminate immediately upon receiving a signal on the `quit` channel.

# Instructions

1. Clone the repository and navigate to the `ws` directory.
2. Put your code in the appropritate methods/files.
   1. Solution with channels goes in `channelAggregator` in `channel_aggregator.go`.
   2. Solution with mutexes goes in `mutexAggregator` in `mutex_aggregator.go`.
3. Run the tests in `ws_test.go`:
   1. `go test -v -race`
   2. Your IDE should also be able to run the tests and show you the results in a GUI. (Note that if you're using VSCode, you will likely need to edit the default go test timeout to be more than 30s.)
   3. For Windows users, you will need WSL set up to run the tests.
4. Upload the `channel_aggregator.go` and `mutex_aggregator.go` files to Gradescope. Do not change the file names, or the autograder may not recognize them.
