// const readline = require("readline")
const fs = require("fs")
const cluster = require("cluster")
const os = require("os")
// const { performance } = require("perf_hooks")

const numCPUs = os.cpus().length
let limitCounter = 0

const dataFolderPath =
  "./src/data/ticks/daily-ft-2/combined-tokens/Backtest tokens/Current-Tokens-In-Backtest"

// "./src/data/ticks/daily-ft-2/combined-tokens/Backtest tokens/CE ATM/All-Tokens/currentTestTokens-6"

const extractTokenInfo = (fileName) => {
  const parts = fileName.split("-")
  const [day, month, year] = parts.slice(1, 4)
  const tokenNumber = parts[5]
  const tokenDate = `${day}-${month}-${year}`
  console.log("token date and number : ", tokenDate, tokenNumber)
  return { tokenDate, tokenNumber }
}

const getTokenDatesAndNumbers = () => {
  return new Promise((resolve, reject) => {
    fs.readdir(dataFolderPath, (err, files) => {
      if (err) {
        reject(err)
      } else {
        const tokenInfoArray = files
          .filter((fileName) => {
            const regex = /^ticks-\d{2}-\d{2}-\d{2}-tk-\d+/
            return regex.test(fileName)
          })
          .map(extractTokenInfo)
        const tokenDates = [
          ...new Set(tokenInfoArray.map((info) => info.tokenDate)),
        ]
        const tokenNumbers = [
          ...new Set(tokenInfoArray.map((info) => info.tokenNumber)),
        ]
        resolve({ tokenDates, tokenNumbers })
      }
    })
  })
}

const calculateAverageVolume = (volumes, tickTime) => {
  volumes = volumes.filter((v) => tickTime - v.time <= 5 * 60 * 1000)

  const totalVolume = volumes.reduce(
    (total, volume) => total + Number(volume.volume_traded),
    0
  )

  const averageVolume = totalVolume / volumes.length

  return averageVolume * 60
}

const getIntervalIndex = (timestamp) => {
  const time = new Date(timestamp)
  const hour = time.getHours()
  const minute = time.getMinutes()

  if ((hour === 9 && minute >= 15) || (hour === 10 && minute <= 30)) {
    return 1
  } else if (
    (hour >= 10 && minute > 30) ||
    hour < 13 ||
    (hour === 13 && minute <= 30)
  ) {
    return 2
  } else if (
    (hour > 13 && minute > 30) ||
    hour < 15 ||
    (hour === 15 && minute <= 30)
  ) {
    return 3
  }

  return 0
}

let startValue = 0
let stopLoss = 0
let target = 0
let lastVolume = 0
let volumes = []
let tradeActive = false

let lowestPrice = 0
let lastPrice = 0
let targetReached = false

// const averageMultipliers = [0.5, 1.5]
// const targetPercentageArray = [0.05, 0.1]
// const slPercentageArray = [0.1, 0.2]
// const trialStopLossPercentageArray = [0.02, 0.01]

const averageMultipliers = [
  0.5, 0.75, 1, 1.25, 1.5, 1.75, 2, 2.25, 2.5, 2.75, 3,
]
const targetPercentageArray = [
  0.03, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.45,
]
const slPercentageArray = [0.02, 0.03, 0.05, 0.07, 0.09, 0.11, 0.15, 0.2]
const trialStopLossPercentageArray = [
  0.005, 0.01, 0.02, 0.03, 0.05, 0.07, 0.1, 0.15, 0.2, 0.25,
]

// const averageMultipliers = [0.5]
// const targetPercentageArray = [0.03]
// const slPercentageArray = [0.02]
// const trialStopLossPercentageArray = [0.005]

// 32103
const prepareJobQueue = (
  tokenDates,
  tokenNumbers,
  averageMultipliers,
  targetPercentageArray,
  slPercentageArray,
  trialStopLossPercentageArray
) => {
  const jobs = []
  for (const tokenDate of tokenDates) {
    for (const averageMultiplier of averageMultipliers) {
      for (const tokenNumber of tokenNumbers) {
        for (const targetPercentage of targetPercentageArray) {
          for (const slPercentage of slPercentageArray) {
            for (const trialStopLossPercentage of trialStopLossPercentageArray) {
              jobs.push({
                tokenDate,
                averageMultiplier,
                tokenNumber,
                targetPercentage,
                slPercentage,
                trialStopLossPercentage,
              })
            }
          }
        }
      }
    }
  }

  return jobs
}

const cache = {}
let averageVolumeCache = {}
const thirtySecsVolumeCache = {}

const backtest = async (job) => {
  const {
    tokenDate,
    averageMultiplier,
    tokenNumber,
    targetPercentage,
    slPercentage,
    trialStopLossPercentage,
  } = job

  startValue = 0
  stopLoss = 0
  target = 0
  lastVolume = 0
  volumes = []
  tradeActive = false

  lowestPrice = 0
  lastPrice = 0

  let sellTimestamp

  let candleOpenPrice = 0
  let candleStartTime = null
  let tickCounts = 0
  let candleOpenPrices = []
  let previousMinute = null
  let totalLinesProcessed = 0

  // const filePath = `./src/data/ticks/daily-ft-2/combined-tokens/Backtest tokens/CE ATM/All-Tokens/currentTestTokens-6/ticks-${tokenDate}-tk-${tokenNumber}`
  const filePath = `./src/data/ticks/daily-ft-2/combined-tokens/Backtest tokens/Current-Tokens-In-Backtest/ticks-${tokenDate}-tk-${tokenNumber}`

  if (fs.existsSync(filePath)) {
    console.log(`exists sync check filepath : ${filePath}`)
    let data = fs.readFileSync(filePath, "utf8")
    data = data.replace(/,(\s*]\s*})$/, "$1")
    try {
      cache[filePath] = JSON.parse(data)
    } catch (err) {
      console.error(`Error parsing JSON from ${filePath}: ${err.message}`)
    }
  }

  averageVolumeCachePath = `./src/data/caches/averageVolumeCache-${tokenDate}-${tokenNumber}.json`
  thirtySecsVolumeCachePath = `./src/data/caches/thirtySecsVolumeCache-${tokenDate}-${tokenNumber}.json`

  let averageVolumeCache
  let thirtySecsVolumeCache

  // console.log(
  //   `checked filePath`,
  //   averageVolumeCachePath,
  //   "and out",
  //   fs.existsSync(averageVolumeCachePath)
  // )

  if (
    fs.existsSync(averageVolumeCachePath) &&
    fs.existsSync(thirtySecsVolumeCachePath)
  ) {
    // console.log("exists sync is working inside")
    averageVolumeCache = JSON.parse(
      fs.readFileSync(averageVolumeCachePath, "utf8")
    )
    thirtySecsVolumeCache = JSON.parse(
      fs.readFileSync(thirtySecsVolumeCachePath, "utf8")
    )
  }

  const processLines = async () => {
    try {
      let totalProfit = 0
      let totalTrades = 0

      let targetReachedCount = 0

      let totalProfitInterval1 = 0
      let totalProfitInterval2 = 0
      let totalProfitInterval3 = 0

      let totalTradesInterval1 = 0
      let totalTradesInterval2 = 0
      let totalTradesInterval3 = 0

      const filePath = `./src/data/ticks/daily-ft-2/combined-tokens/Backtest tokens/Current-Tokens-In-Backtest/ticks-${tokenDate}-tk-${tokenNumber}`

      if (!fs.existsSync(filePath)) {
        // console.log("filepath not found, returning", filePath)
        return
      }

      let tickData

      let results = []
      let tradeDetails = []

      if (cache[filePath]) {
        tickData = cache[filePath]
      } else {
        console.log(
          "file being read, cache not used, this shouldn't have happened"
        )
        let data = fs.readFileSync(filePath, "utf8")

        try {
          tickData = JSON.parse(data)
        } catch (error) {
          console.log("error parsing file", error)
          return
        }

        cache[filePath] = tickData
      }

      for (let date in tickData) {
        const lines = tickData[date]

        for (let line of lines) {
          let tick = line

          tickCounts = tickCounts + 1

          const tickTime = new Date(tick.time)

          const tickMinute = new Date(
            tickTime.getFullYear(),
            tickTime.getMonth(),
            tickTime.getDate(),
            tickTime.getHours(),
            tickTime.getMinutes()
          )

          if (
            !candleOpenPrices.find(
              (entry) => entry.minute.getTime() === tickMinute.getTime()
            )
          ) {
            if (tick.lp !== undefined) {
              candleOpenPrices.push({
                minute: tickMinute,
                openPrice: tick.lp,
              })
            }
          }

          candleOpenPrices = candleOpenPrices.filter(
            (entry) => tickTime - entry.minute <= 5 * 60 * 1000
          )

          const currentCandle = candleOpenPrices.find(
            (entry) => entry.minute.getTime() === tickMinute.getTime()
          )

          const candleOpenPrice = currentCandle ? currentCandle.openPrice : null

          const dateStamp = new Date(tick.time).toLocaleDateString()

          lastPrice = tick.lp

          // averageVolume =
          //   averageVolumeCache[
          //     `${tickTime.getTime()}-${tokenNumber}-${tokenDate}`
          //   ]
          // thirtySecondVolume =
          //   thirtySecsVolumeCache[
          //     `${tickTime.getTime()}-${tokenNumber}-${tokenDate}`
          //   ]

          // if (limitCounter > 40 && limitCounter < 42) {
          //   console.log(averageVolumeCache)
          // }
          // limitCounter++

          // console.log(
          //   "time",
          //   `${tickTime.getTime()}`,
          //   `and avg vol cache at time`
          // )

          // Check if averageVolumeCache exists and contains the needed property
          if (
            averageVolumeCache &&
            averageVolumeCache.hasOwnProperty(`${tickTime.getTime()}`)
          ) {
            // console.log("read from cachesssssss")
            averageVolume = averageVolumeCache[`${tickTime.getTime()}`]
          } else {
            console.log(
              `Missing data in averageVolumeCache for time: ${tickTime.getTime()} and token date : ${tokenDate} and token number : ${tokenNumber}`
            )
          }

          // Check if thirtySecsVolumeCache exists and contains the needed property
          if (
            thirtySecsVolumeCache &&
            thirtySecsVolumeCache.hasOwnProperty(`${tickTime.getTime()}`)
          ) {
            thirtySecondVolume = thirtySecsVolumeCache[`${tickTime.getTime()}`]
          } else {
            console.log(
              `Missing data in thirtySecsVolumeCache for time: ${tickTime.getTime()}, and token date : ${tokenDate} and token number : ${tokenNumber}`
            )
          }

          // averageVolume = averageVolumeCache[`${tickTime.getTime()}`]
          // thirtySecondVolume = thirtySecsVolumeCache[`${tickTime.getTime()}`]

          if (!tradeActive && tickCounts > 300) {
            if (
              thirtySecondVolume > averageMultiplier * averageVolume &&
              lastPrice > startValue
            ) {
              if (lastPrice >= candleOpenPrice) {
                continue
              }

              sellTimestamp = tick.time

              startValue = lastPrice
              stopLoss = lastPrice * (1 + slPercentage)
              target = lastPrice * (1 - targetPercentage)
              lowestPrice = lastPrice
              tradeActive = true

              const intervalIndex = getIntervalIndex(tick.time)

              if (intervalIndex === 1) totalTradesInterval1 += 1
              if (intervalIndex === 2) totalTradesInterval2 += 1
              if (intervalIndex === 3) totalTradesInterval3 += 1

              totalTrades++
            }
          }

          if (tradeActive && lastPrice <= target) {
            targetReached = true
          }

          if (tradeActive && lastPrice <= lowestPrice && targetReached) {
            lowestPrice = lastPrice
            stopLoss = lowestPrice * (1 + trialStopLossPercentage)
          }

          if (tradeActive && lastPrice >= stopLoss) {
            if (targetReached) {
              targetReachedCount++
              targetReached = false
            }

            const profit = startValue - lastPrice
            const intervalIndex = getIntervalIndex(tick.time)

            if (intervalIndex === 1) totalProfitInterval1 += profit
            if (intervalIndex === 2) totalProfitInterval2 += profit
            if (intervalIndex === 3) totalProfitInterval3 += profit

            totalProfit += profit

            // if (
            //   averageMultiplier == `0.5` &&
            //   targetPercentage == `0.25` &&
            //   slPercentage == `0.15` &&
            //   trialStopLossPercentage == `0.25`
            // ) {
            // }

            const sellTime = new Date(sellTimestamp)
            const buyTime = new Date(tick.time)

            const holdTimeSeconds = (buyTime - sellTime) / 1000

            const formatTime = (time) => {
              return `${time.getHours().toString().padStart(2, "0")}:${time
                .getMinutes()
                .toString()
                .padStart(2, "0")}:${time
                .getSeconds()
                .toString()
                .padStart(2, "0")}`
            }

            const holdTime = new Date(holdTimeSeconds * 1000)
              .toISOString()
              .substr(11, 8)

            tradeDetails.push({
              tokenNumber,
              tokenDate,
              dateStamp,
              sellTime: formatTime(sellTime),
              startValue,
              buyTime: formatTime(buyTime),
              lastPrice,
              profit: profit.toFixed(2),
              holdTime,
            })

            startValue = 0
            stopLoss = 0
            target = 0
            lowestPrice = 0
            volumes = []
            tradeActive = false
          }
        }

        results.push({
          tokenNumber,
          tokenDate,
          trialStopLossPercentage,
          averageMultiplier,
          targetPercentage,
          slPercentage,
          totalProfit: totalProfit.toFixed(2),
          totalTrades,
          tickCounts,
          totalTradesInterval1,
          totalProfitInterval1: totalProfitInterval1.toFixed(2),
          totalTradesInterval2,
          totalProfitInterval2: totalProfitInterval2.toFixed(2),
          totalTradesInterval3,
          totalProfitInterval3: totalProfitInterval3.toFixed(2),
          targetReachedCount,
        })
      }

      // const pathForResults = `./src/data/results/TrailTargetPercentageIntervalSelling/trade-details/${tokenNumber}-${tokenDate}-${averageMultiplier}-${targetPercentage}-${slPercentage}-${trialStopLossPercentage}-trade-details.csv`

      const pathForResultsDir = `./src/data/results/TrailTargetPercentageIntervalSelling/trade-details/${tokenDate}-${tokenNumber}`
      const pathForResults = `${pathForResultsDir}/${tokenNumber}-${tokenDate}-${averageMultiplier}-${targetPercentage}-${slPercentage}-${trialStopLossPercentage}-trade-details.csv`

      // Check if directory exists, if not create it
      if (!fs.existsSync(pathForResultsDir)) {
        fs.mkdirSync(pathForResultsDir, { recursive: true })
      }

      // if (!fs.existsSync(pathForResults)) {
      fs.writeFile(
        pathForResults,
        `Token Number, Token Date, Date Stamp, Type-1, Sell Time, Start Value, Type-2 , Buy Time, Last Price, Profit, Hold Time\n`,
        "utf8",
        function (err) {
          if (err) {
            throw err
          }
        }
      )

      for (let trade of tradeDetails) {
        fs.appendFileSync(
          pathForResults,
          `${trade.tokenNumber}, ${trade.tokenDate}, ${trade.dateStamp}, Sell, ${trade.sellTime}, ${trade.startValue}, Buy, ${trade.buyTime}, ${trade.lastPrice}, ${trade.profit}, ${trade.holdTime}\n`,
          (err) => {
            if (err) {
              throw err
            }
          }
        )
      }
      // }

      const pathForMainResults = `./src/data/results/TrailTargetPercentageIntervalSelling/${tokenNumber}-${tokenDate}-target-percentage-interval.csv`

      if (!fs.existsSync(pathForMainResults)) {
        console.log(`mainResults path not existing, ${pathForMainResults}`)

        fs.writeFileSync(
          pathForMainResults,
          `Token,CE or PE,Date,Expiry,Day Trend 1st half,Day Trend 2nd half - 1pm to 3.30,SL Trail Points,Average Multiplier,Target,Stop,Profit,Total Trades,Tick Count,Total Trades Interval1,Total Profit Interval1,Total Trades Interval2,Total Profit Interval2,Total Trades Interval3,Total Profit Interval3,Target Reached Count\n`,
          "utf8"
        )
      }

      for (let result of results) {
        fs.appendFileSync(
          pathForMainResults,
          `${result.tokenNumber} , , ${result.tokenDate}, , , , ${result.trialStopLossPercentage} ,${result.averageMultiplier}  ,  ${result.targetPercentage} , ${result.slPercentage}, ${result.totalProfit}, ${result.totalTrades}, ${result.tickCounts}, ${result.totalTradesInterval1} ,${result.totalProfitInterval1}, ${result.totalTradesInterval2} ,${result.totalProfitInterval2}, ${result.totalTradesInterval3} ,${result.totalProfitInterval3}, ${result.targetReachedCount}` +
            `\n`,
          (err) => {
            if (err) {
              throw err
            }
          }
        )
      }

      // if (
      //   averageMultiplier == `0.5` &&
      //   targetPercentage == `0.25` &&
      //   slPercentage == `0.15` &&
      //   trialStopLossPercentage == `0.25`
      // ) {
      //   fs.writeFileSync(
      //     `./src/data/averageVolumeCacheTempText.txt`,
      //     JSON.stringify(averageVolumeCache)
      //   )
      //   fs.writeFileSync(
      //     `./src/data/thirtySecsVolumeCacheTempText.txt`,
      //     JSON.stringify(thirtySecsVolumeCache)
      //   )
      // }
    } catch (innerError) {
      console.error("An error occurred during loop execution:", innerError)
    }
  }

  try {
    await processLines()
  } catch (error) {
    console.error("An error occurred in processlines await:", error)
  }
}

// const prepareCache = async (tokenDates, tokenNumbers) => {
//   console.log("Starting to prepare cache...")

//   let fileCount = 0
//   let loadedFiles = 0
//   let volumes = []
//   let lastVolume = null

//   for (const tokenDate of tokenDates) {
//     for (const tokenNumber of tokenNumbers) {
//       const filePath = `./src/data/ticks/daily-ft-2/combined-tokens/Backtest tokens/CE ATM/All-Tokens/currentTestTokens-6/ticks-${tokenDate}-tk-${tokenNumber}`

//       if (!fs.existsSync(filePath)) {
//         continue
//       }

//       fileCount += 1

//       const data = fs.readFileSync(filePath, "utf8")

//       const tickData = JSON.parse(data)

//       for (let date in tickData) {
//         const lines = tickData[date]
//         for (let line of lines) {
//           let tick = line

//           const tickTime = new Date(tick.time)

//           let volume = 0
//           if (tick.v !== undefined) {
//             volume = tick.v - (lastVolume || 0)
//             lastVolume = tick.v
//           } else {
//             volume = 0
//           }

//           volumes.push({ time: tickTime, volume_traded: volume })

//           let averageVolume

//           averageVolume = calculateAverageVolume(volumes, tickTime)

//           averageVolumeCache[
//             `${tickTime.getTime()}-${tokenNumber}-${tokenDate}`
//           ] = averageVolume

//           const last30SecondVolumes = volumes.filter(
//             (v) => tickTime - v.time <= 30 * 1000
//           )
//           thirtySecondVolume = last30SecondVolumes.reduce(
//             (total, volume) => total + Number(volume.volume_traded),
//             0
//           )

//           thirtySecsVolumeCache[
//             `${tickTime.getTime()}-${tokenNumber}-${tokenDate}`
//           ] = thirtySecondVolume
//         }
//         volumes = []
//       }

//       cache[filePath] = tickData

//       loadedFiles += 1
//       console.log(`Cached file: ${loadedFiles} / ${fileCount}`)

//       if (loadedFiles === fileCount) {
//         console.log("Cache prepared.")
//       }
//     }
//   }
//   fs.writeFileSync(
//     "./src/data/averageVolumeCache.json",
//     JSON.stringify(averageVolumeCache)
//   )
//   fs.writeFileSync(
//     "./src/data/thirtySecsVolumeCache.json",
//     JSON.stringify(thirtySecsVolumeCache)
//   )
// }

const prepareCache = async (tokenDates, tokenNumbers) => {
  console.log("Starting to prepare cache...")

  let fileCount = 0
  let loadedFiles = 0
  let volumes = []
  let lastVolume = null

  console.log("this is running", tokenDates, tokenNumbers)

  for (const tokenDate of tokenDates) {
    for (const tokenNumber of tokenNumbers) {
      const averageVolumeCachePath = `./src/data/caches/averageVolumeCache-${tokenDate}-${tokenNumber}.json`
      const thirtySecsVolumeCachePath = `./src/data/caches/thirtySecsVolumeCache-${tokenDate}-${tokenNumber}.json`

      // if (fs.existsSync(averageVolumeCachePath)) {
      //   console.log(`Cache file does exist: ${averageVolumeCachePath}`)
      // }
      // if (fs.existsSync(thirtySecsVolumeCachePath)) {
      //   console.log(`Cache file does not exist: ${thirtySecsVolumeCachePath}`)
      // }
      // if (fs.existsSync(filePath)) {
      //   console.log(`Data file does not exist: ${filePath}`)
      // }

      // If both cache files exist, skip to next iteration
      if (
        fs.existsSync(averageVolumeCachePath) &&
        fs.existsSync(thirtySecsVolumeCachePath)
      ) {
        console.log(
          "avg volume and thirtySecsVolume already found in caches, so continuing"
        )
        continue
      }

      let averageVolumeCache = {} // Reset cache for each tokenNumber
      let thirtySecsVolumeCache = {}

      // const filePath = `./src/data/ticks/daily-ft-2/combined-tokens/Backtest tokens/CE ATM/All-Tokens/currentTestTokens-6/ticks-${tokenDate}-tk-${tokenNumber}`

      const filePath = `./src/data/ticks/daily-ft-2/combined-tokens/Backtest tokens/Current-Tokens-In-Backtest/ticks-${tokenDate}-tk-${tokenNumber}`

      if (!fs.existsSync(filePath)) {
        continue
      }

      fileCount += 1

      console.log("file being cached", filePath)

      let data = fs.readFileSync(filePath, "utf8")

      // Remove the trailing comma from the JSON array
      data = data.replace(/,(\s*]\s*})$/, "$1")

      const tickData = JSON.parse(data)

      for (let date in tickData) {
        const lines = tickData[date]
        for (let line of lines) {
          let tick = line

          const tickTime = new Date(tick.time)

          let volume = 0
          if (tick.v !== undefined) {
            volume = tick.v - (lastVolume || 0)
            lastVolume = tick.v
          } else {
            volume = 0
          }

          volumes.push({ time: tickTime, volume_traded: volume })

          let averageVolume
          averageVolume = calculateAverageVolume(volumes, tickTime)

          averageVolumeCache[`${tickTime.getTime()}`] = averageVolume

          const last30SecondVolumes = volumes.filter(
            (v) => tickTime - v.time <= 30 * 1000
          )
          thirtySecondVolume = last30SecondVolumes.reduce(
            (total, volume) => total + Number(volume.volume_traded),
            0
          )

          thirtySecsVolumeCache[`${tickTime.getTime()}`] = thirtySecondVolume
        }
        volumes = []
      }

      cache[filePath] = tickData

      loadedFiles += 1
      console.log(`Cached file: ${loadedFiles} / ${fileCount}`)

      // Save each cache to a unique file
      fs.writeFileSync(
        averageVolumeCachePath,
        JSON.stringify(averageVolumeCache)
      )
      fs.writeFileSync(
        thirtySecsVolumeCachePath,
        JSON.stringify(thirtySecsVolumeCache)
      )
    }
  }
  console.log("Cache prepared.")
}

const init = async () => {
  if (cluster.isMaster) {
    console.time("Execution time")

    getTokenDatesAndNumbers()
      .then(({ tokenDates, tokenNumbers }) => {
        return prepareCache(tokenDates, tokenNumbers).then(() => ({
          tokenDates,
          tokenNumbers,
        }))
      })
      .then(({ tokenDates, tokenNumbers }) => {
        const jobs = prepareJobQueue(
          tokenDates,
          tokenNumbers,
          averageMultipliers,
          targetPercentageArray,
          slPercentageArray,
          trialStopLossPercentageArray
        )

        // for (let i = 0; i < numCPUs; i++) {
        //   const worker = cluster.fork()
        //   worker.send(jobs)
        // }

        // cluster.on("exit", (worker, code, signal) => {
        //   console.log(`Worker ${worker.process.pid} finished`)
        // })

        for (let i = 0; i < numCPUs; i++) {
          const worker = cluster.fork()

          // When worker is online, send it the first job
          worker.on("online", () => {
            const job = jobs.pop()
            if (job) worker.send(job)
          })

          // When worker sends a message that it's done, send it a new job
          worker.on("message", () => {
            const job = jobs.pop()
            if (job) worker.send(job)
            else worker.kill()
          })
        }

        // // When worker is online, send it the first job
        // cluster.on("online", (worker) => {
        //   const job = jobs.pop()
        //   if (job) worker.send(job)
        // })

        // // When worker sends a message that it's done, send it a new job
        // cluster.on("message", (worker) => {
        //   const job = jobs.pop()
        //   if (job) worker.send(job)
        //   else worker.kill()
        // })
      })
  } else {
    // process.on("message", async (jobs) => {
    //   const chunkSize = Math.ceil(jobs.length / numCPUs)
    //   const workerIndex = cluster.worker.id - 1
    //   const jobsChunk = jobs.slice(
    //     workerIndex * chunkSize,
    //     (workerIndex + 1) * chunkSize
    //   )
    //   for (const job of jobsChunk) {
    //     await backtest(job)
    //   }
    //   console.log(`Worker ${process.pid} completed its task.`)
    //   process.exit(0)
    // })
    process.on("message", async (job) => {
      await backtest(job)
      // Tell the master process that we're ready for another job
      process.send("ready")
    })
  }
}

init()

// With logging and extra comments :

// const readline = require("readline")
// const fs = require("fs")
// const cluster = require("cluster")
// const os = require("os")
// const { performance } = require("perf_hooks")

// const numCPUs = os.cpus().length

// const dataFolderPath =
//   "./src/data/ticks/daily-ft-2/combined-tokens/Backtest tokens/CE ATM/All-Tokens/currentTestTokens-6"

// const extractTokenInfo = (fileName) => {
//   const parts = fileName.split("-")
//   const [day, month, year] = parts.slice(1, 4)
//   const tokenNumber = parts[5]
//   const tokenDate = `${day}-${month}-${year}`
//   console.log("token date and number : ", tokenDate, tokenNumber)
//   return { tokenDate, tokenNumber }
// }

// const getTokenDatesAndNumbers = () => {
//   return new Promise((resolve, reject) => {
//     fs.readdir(dataFolderPath, (err, files) => {
//       if (err) {
//         reject(err)
//       } else {
//         const tokenInfoArray = files
//           .filter((fileName) => {
//             const regex = /^ticks-\d{2}-\d{2}-\d{2}-tk-\d+/
//             return regex.test(fileName)
//           })
//           .map(extractTokenInfo)
//         const tokenDates = [
//           ...new Set(tokenInfoArray.map((info) => info.tokenDate)),
//         ]
//         const tokenNumbers = [
//           ...new Set(tokenInfoArray.map((info) => info.tokenNumber)),
//         ]
//         resolve({ tokenDates, tokenNumbers })
//       }
//     })
//   })
// }

// const calculateAverageVolume = (volumes, tickTime) => {
//   volumes = volumes.filter((v) => tickTime - v.time <= 5 * 60 * 1000)

//   const totalVolume = volumes.reduce(
//     (total, volume) => total + Number(volume.volume_traded),
//     0
//   )

//   const averageVolume = totalVolume / volumes.length

//   return averageVolume * 60
// }

// const getIntervalIndex = (timestamp) => {
//   const time = new Date(timestamp)
//   const hour = time.getHours()
//   const minute = time.getMinutes()

//   if ((hour === 9 && minute >= 15) || (hour === 10 && minute <= 30)) {
//     return 1
//   } else if (
//     (hour >= 10 && minute > 30) ||
//     hour < 13 ||
//     (hour === 13 && minute <= 30)
//   ) {
//     return 2
//   } else if (
//     (hour > 13 && minute > 30) ||
//     hour < 15 ||
//     (hour === 15 && minute <= 30)
//   ) {
//     return 3
//   }

//   return 0
// }

// let startValue = 0
// let stopLoss = 0
// let target = 0
// let lastVolume = 0
// let volumes = []
// let tradeActive = false

// let lowestPrice = 0
// let lastPrice = 0
// let targetReached = false

// // const averageMultipliers = [0.5, 0.75]
// // const targetPercentageArray = [0.25, 0.3]
// // const slPercentageArray = [0.15, 0.2]
// // const trialStopLossPercentageArray = [0.25, 0.45]

// //  tokenDate == `04-05-23` &&
// //    averageMultiplier == `0.5` &&
// //    tokenNumber == `56854` &&
// //    targetPercentage == `0.25` &&
// //    slPercentage == `0.15` &&
// //    trialStopLossPercentage == `0.25`

// const averageMultipliers = [
//   0.5, 0.75, 1, 1.25, 1.5, 1.75, 2, 2.25, 2.5, 2.75, 3,
// ]
// const targetPercentageArray = [
//   0.03, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.45,
// ]
// const slPercentageArray = [0.02, 0.03, 0.05, 0.07, 0.09, 0.11, 0.15, 0.2]
// const trialStopLossPercentageArray = [
//   0.005, 0.01, 0.02, 0.03, 0.05, 0.07, 0.1, 0.15, 0.2, 0.25,
// ]

// const prepareJobQueue = (
//   tokenDates,
//   tokenNumbers,
//   averageMultipliers,
//   targetPercentageArray,
//   slPercentageArray,
//   trialStopLossPercentageArray
// ) => {
//   const jobs = []
//   for (const tokenDate of tokenDates) {
//     for (const averageMultiplier of averageMultipliers) {
//       for (const tokenNumber of tokenNumbers) {
//         for (const targetPercentage of targetPercentageArray) {
//           for (const slPercentage of slPercentageArray) {
//             for (const trialStopLossPercentage of trialStopLossPercentageArray) {
//               jobs.push({
//                 tokenDate,
//                 averageMultiplier,
//                 tokenNumber,
//                 targetPercentage,
//                 slPercentage,
//                 trialStopLossPercentage,
//               })
//             }
//           }
//         }
//       }
//     }
//   }

//   return jobs
// }

// const cache = {}
// let averageVolumeCache = {}
// const thirtySecsVolumeCache = {}

// const backtest = async (job) => {
//   const {
//     tokenDate,
//     averageMultiplier,
//     tokenNumber,
//     targetPercentage,
//     slPercentage,
//     trialStopLossPercentage,
//   } = job

//   startValue = 0
//   stopLoss = 0
//   target = 0
//   lastVolume = 0
//   volumes = []
//   tradeActive = false

//   lowestPrice = 0
//   lastPrice = 0

//   let sellTimestamp

//   let candleOpenPrice = 0
//   let candleStartTime = null
//   let tickCounts = 0
//   let candleOpenPrices = []
//   let previousMinute = null
//   let totalLinesProcessed = 0

//   const filePath = `./src/data/ticks/daily-ft-2/combined-tokens/Backtest tokens/CE ATM/All-Tokens/currentTestTokens-6/ticks-${tokenDate}-tk-${tokenNumber}`

//   let data = fs.readFileSync(filePath, "utf8")
//   cache[filePath] = JSON.parse(data)

//   const averageVolumeCache = JSON.parse(
//     fs.readFileSync("./src/data/averageVolumeCache.json", "utf8")
//   )
//   const thirtySecsVolumeCache = JSON.parse(
//     fs.readFileSync("./src/data/thirtySecsVolumeCache.json", "utf8")
//   )

//   const processLines = async () => {
//     try {
//       let totalProfit = 0
//       let totalTrades = 0

//       let targetReachedCount = 0

//       let totalProfitInterval1 = 0
//       let totalProfitInterval2 = 0
//       let totalProfitInterval3 = 0

//       let totalTradesInterval1 = 0
//       let totalTradesInterval2 = 0
//       let totalTradesInterval3 = 0

//       const filePath = `./src/data/ticks/daily-ft-2/combined-tokens/Backtest tokens/CE ATM/All-Tokens/currentTestTokens-6/ticks-${tokenDate}-tk-${tokenNumber}`

//       if (!fs.existsSync(filePath)) {
//         return
//       }

//       let tickData

//       let results = []
//       let tradeDetails = []

//       if (cache[filePath]) {
//         tickData = cache[filePath]
//       } else {
//         console.log(
//           "file being read, cache not used, this shouldn't have happened"
//         )
//         let data = fs.readFileSync(filePath, "utf8")

//         try {
//           tickData = JSON.parse(data)
//         } catch (error) {
//           console.log("error parsing file", error)
//           return
//         }

//         cache[filePath] = tickData
//       }

//       for (let date in tickData) {
//         const lines = tickData[date]

//         for (let line of lines) {
//           // totalLinesProcessed++
//           let tick = line

//           tickCounts = tickCounts + 1

//           const tickTime = new Date(tick.time)

//           const tickMinute = new Date(
//             tickTime.getFullYear(),
//             tickTime.getMonth(),
//             tickTime.getDate(),
//             tickTime.getHours(),
//             tickTime.getMinutes()
//           )

//           if (
//             !candleOpenPrices.find(
//               (entry) => entry.minute.getTime() === tickMinute.getTime()
//             )
//           ) {
//             if (tick.lp !== undefined) {
//               candleOpenPrices.push({
//                 minute: tickMinute,
//                 openPrice: tick.lp,
//               })
//             }
//           }

//           candleOpenPrices = candleOpenPrices.filter(
//             (entry) => tickTime - entry.minute <= 5 * 60 * 1000
//           )

//           const currentCandle = candleOpenPrices.find(
//             (entry) => entry.minute.getTime() === tickMinute.getTime()
//           )

//           const candleOpenPrice = currentCandle ? currentCandle.openPrice : null

//           const dateStamp = new Date(tick.time).toLocaleDateString()

//           // let volume = 0
//           // if (tick.v !== undefined) {
//           //   volume = tick.v - (lastVolume || 0)
//           //   lastVolume = tick.v
//           // } else {
//           //   volume = 0
//           // }

//           // volumes.push({ time: tickTime, volume_traded: volume })

//           lastPrice = tick.lp

//           // if (tickCounts < 2)
//           //   console.log("this inside, averageVolumeCache", averageVolumeCache)

//           // console.log(
//           //   "avg vol cache inputs",
//           //   `${tickTime.getTime()}-${tokenNumber}-${tokenDate}`,
//           //   `and thirtySecondVolume input`,
//           //   `${tick.time}-${tokenNumber}-${tokenDate}`,
//           //   `and averageVolume`,
//           //   averageVolumeCache[
//           //     `${tickTime.getTime()}-${tokenNumber}-${tokenDate}`
//           //   ],
//           //   `and thirty sec volume`,
//           //   thirtySecsVolumeCache[`${tick.time}-${tokenNumber}-${tokenDate}`]
//           // )

//           // averageVolume =
//           //   averageVolumeCache[
//           //     `${tickTime.getTime()}-${tokenNumber}-${tokenDate}`
//           //   ]
//           // thirtySecondVolume =
//           //   thirtySecsVolumeCache[`${tick.time}-${tokenNumber}-${tokenDate}`]
//           averageVolume =
//             averageVolumeCache[
//               `${tickTime.getTime()}-${tokenNumber}-${tokenDate}`
//             ]
//           thirtySecondVolume =
//             thirtySecsVolumeCache[
//               `${tickTime.getTime()}-${tokenNumber}-${tokenDate}`
//             ]

//           // if (true) {
//           //   throw new error()
//           // }

//           // let averageVolume
//           // if (averageVolumeCache[`${tickTime.getTime()}-${tokenNumber}`]) {
//           // averageVolume =
//           //   averageVolumeCache[`${tickTime.getTime()}-${tokenNumber}`]
//           // } else {
//           //   console.log("average volume cached at time", tickTime.getTime())
//           //   averageVolume = calculateAverageVolume(volumes, tickTime)
//           //   averageVolumeCache[`${tickTime.getTime()}-${tokenNumber}`] =
//           //     averageVolume
//           // }

//           // if (
//           //   thirtySecsVolumeCache.hasOwnProperty(`${tick.time}-${tokenNumber}`)
//           // ) {
//           // thirtySecondVolume =
//           //   thirtySecsVolumeCache[`${tick.time}-${tokenNumber}`]
//           // } else {
//           //   const last30SecondVolumes = volumes.filter(
//           //     (v) => tickTime - v.time <= 30 * 1000
//           //   )
//           //   thirtySecondVolume = last30SecondVolumes.reduce(
//           //     (total, volume) => total + Number(volume.volume_traded),
//           //     0
//           //   )
//           //   console.log("30 sec volume cached")
//           //   thirtySecsVolumeCache[`${tick.time}-${tokenNumber}`] =
//           //     thirtySecondVolume
//           // }

//           // console.log("volumes.length", volumes.length) -- Disabled to check volume length to be greater than 300 as now using cache, so instead starting to use tick counts now

//           if (!tradeActive && tickCounts > 300) {
//             // console.log(`condition 1 entered`)
//             // console.log(
//             //   "condition 1 entered, thirty second volume, avg multipier, and averageVolume, lastPrice, startValue and candleOpenPrice",
//             //   thirtySecondVolume,
//             //   averageMultiplier,
//             //   averageVolume,
//             //   lastPrice,
//             //   startValue,
//             //   candleOpenPrice
//             // )

//             if (
//               thirtySecondVolume > averageMultiplier * averageVolume &&
//               lastPrice > startValue
//             ) {
//               // console.log("condition 2 entered")
//               if (lastPrice >= candleOpenPrice) {
//                 // console.log("condition 3 entered")
//                 continue
//               }
//               // console.log("condition 4 entered")
//               sellTimestamp = tick.time

//               startValue = lastPrice
//               stopLoss = lastPrice * (1 + slPercentage)
//               target = lastPrice * (1 - targetPercentage)
//               lowestPrice = lastPrice
//               tradeActive = true

//               const intervalIndex = getIntervalIndex(tick.time)
//               // console.log("condition 5 entered")

//               if (intervalIndex === 1) totalTradesInterval1 += 1
//               if (intervalIndex === 2) totalTradesInterval2 += 1
//               if (intervalIndex === 3) totalTradesInterval3 += 1

//               totalTrades++
//             }
//             // console.log("condition 6 entered")
//           }
//           // console.log("condition 7 entered")
//           if (tradeActive && lastPrice <= target) {
//             // console.log("condition 8 entered")
//             targetReached = true
//           }

//           if (tradeActive && lastPrice <= lowestPrice && targetReached) {
//             // console.log("condition 9 entered")
//             lowestPrice = lastPrice
//             stopLoss = lowestPrice * (1 + trialStopLossPercentage)
//           }

//           if (tradeActive && lastPrice >= stopLoss) {
//             // console.log("condition 10 entered")
//             if (targetReached) {
//               // console.log("condition 11 entered")
//               targetReachedCount++
//               targetReached = false
//             }
//             // console.log("condition 12 entered")
//             const profit = startValue - lastPrice
//             const intervalIndex = getIntervalIndex(tick.time)

//             if (intervalIndex === 1) totalProfitInterval1 += profit
//             if (intervalIndex === 2) totalProfitInterval2 += profit
//             if (intervalIndex === 3) totalProfitInterval3 += profit

//             totalProfit += profit
//             // 56854-04-05-23-0.5-0.25-0.15-0.25
//             if (
//               averageMultiplier == `0.5` &&
//               targetPercentage == `0.25` &&
//               slPercentage == `0.15` &&
//               trialStopLossPercentage == `0.25`
//             ) {
//               // console.log("condition 13 entered")
//               // console.log("profit and totalprofit now", profit, totalProfit)
//             }

//             const sellTime = new Date(sellTimestamp)
//             const buyTime = new Date(tick.time)
//             // console.log("condition 14 entered")
//             const holdTimeSeconds = (buyTime - sellTime) / 1000

//             const formatTime = (time) => {
//               // console.log("condition 15 entered")
//               return `${time.getHours().toString().padStart(2, "0")}:${time
//                 .getMinutes()
//                 .toString()
//                 .padStart(2, "0")}:${time
//                 .getSeconds()
//                 .toString()
//                 .padStart(2, "0")}`
//             }

//             const holdTime = new Date(holdTimeSeconds * 1000)
//               .toISOString()
//               .substr(11, 8)

//             tradeDetails.push({
//               tokenNumber,
//               tokenDate,
//               dateStamp,
//               sellTime: formatTime(sellTime),
//               startValue,
//               buyTime: formatTime(buyTime),
//               lastPrice,
//               profit: profit.toFixed(2),
//               holdTime,
//             })

//             startValue = 0
//             stopLoss = 0
//             target = 0
//             lowestPrice = 0
//             volumes = []
//             tradeActive = false
//           }
//           // console.log("condition 16 entered")
//         }
//         // console.log("condition 17 entered")
//         results.push({
//           tokenNumber,
//           tokenDate,
//           trialStopLossPercentage,
//           averageMultiplier,
//           targetPercentage,
//           slPercentage,
//           totalProfit: totalProfit.toFixed(2),
//           totalTrades,
//           tickCounts,
//           totalTradesInterval1,
//           totalProfitInterval1: totalProfitInterval1.toFixed(2),
//           totalTradesInterval2,
//           totalProfitInterval2: totalProfitInterval2.toFixed(2),
//           totalTradesInterval3,
//           totalProfitInterval3: totalProfitInterval3.toFixed(2),
//           targetReachedCount,
//         })
//         // console.log("condition 18 entered")
//         // console.log("this results", results)
//       }
//       // console.log("condition 19 entered")
//       const pathForResults = `./src/data/results/TrailTargetPercentageIntervalSelling/trade-details/${tokenNumber}-${tokenDate}-${averageMultiplier}-${targetPercentage}-${slPercentage}-${trialStopLossPercentage}-trade-details.csv`

//       if (!fs.existsSync(pathForResults)) {
//         // console.log("file path for results not existing")
//         fs.writeFileSync(
//           pathForResults,
//           `Token Number, Token Date, Date Stamp, Sell Time, Start Value, Buy Time, Last Price, Profit, Hold Time\n`,
//           "utf8"
//         )
//         // console.log("condition 20 entered")
//         // console.log("trade details", tradeDetails)
//         for (let trade of tradeDetails) {
//           // console.log("condition 21 entered")
//           fs.appendFileSync(
//             pathForResults,
//             `${trade.tokenNumber}, ${trade.tokenDate}, ${trade.dateStamp}, Sell, ${trade.sellTime}, ${trade.startValue}, Buy, ${trade.buyTime}, ${trade.lastPrice}, ${trade.profit}, ${trade.holdTime}\n`,
//             (err) => {
//               if (err) {
//                 throw err
//               }
//             }
//           )
//         }
//       }
//       // console.log("condition 22 entered")
//       const pathForMainResults = `./src/data/results/TrailTargetPercentageIntervalSelling/${tokenNumber}-${tokenDate}-target-percentage-interval.csv`

//       if (!fs.existsSync(pathForMainResults)) {
//         // console.log("condition 23 entered")
//         console.log(`mainResults path not existing`)

//         fs.writeFileSync(
//           pathForMainResults,
//           `Token,CE or PE,Date,Expiry,Day Trend 1st half,Day Trend 2nd half - 1pm to 3.30,SL Trail Points,Average Multiplier,Target,Stop,Profit,Total Trades,Tick Count,Total Trades Interval1,Total Profit Interval1,Total Trades Interval2,Total Profit Interval2,Total Trades Interval3,Total Profit Interval3,Target Reached Count\n`,
//           "utf8"
//         )
//       }
//       // console.log("results in main results", results)
//       for (let result of results) {
//         // console.log("condition 24 entered and result is ", result)
//         fs.appendFileSync(
//           pathForMainResults,
//           `${result.tokenNumber} , , ${result.tokenDate}, , , , ${result.trialStopLossPercentage} ,${result.averageMultiplier}  ,  ${result.targetPercentage} , ${result.slPercentage}, ${result.totalProfit}, ${result.totalTrades}, ${result.tickCounts}, ${result.totalTradesInterval1} ,${result.totalProfitInterval1}, ${result.totalTradesInterval2} ,${result.totalProfitInterval2}, ${result.totalTradesInterval3} ,${result.totalProfitInterval3}, ${result.targetReachedCount}` +
//             `\n`,
//           (err) => {
//             if (err) {
//               throw err
//             }
//           }
//         )
//       }

//       if (
//         averageMultiplier == `0.5` &&
//         targetPercentage == `0.25` &&
//         slPercentage == `0.15` &&
//         trialStopLossPercentage == `0.25`
//       ) {
//         fs.writeFileSync(
//           `./src/data/averageVolumeCacheTempText.txt`,
//           JSON.stringify(averageVolumeCache)
//         )
//         fs.writeFileSync(
//           `./src/data/thirtySecsVolumeCacheTempText.txt`,
//           JSON.stringify(thirtySecsVolumeCache)
//         )
//         // console.log("average vol cache", averageVolumeCache)
//         // console.log("30 sec vol cache", thirtySecsVolumeCache)
//       }

//       // console.log("average vol cache", averageVolumeCache)
//       // console.log("30 sec vol cache", thirtySecsVolumeCache)
//     } catch (innerError) {
//       console.error("An error occurred during loop execution:", innerError)
//     }
//   }

//   try {
//     await processLines()
//   } catch (error) {
//     console.error("An error occurred in processlines await:", error)
//   }
// }

// const prepareCache = async (tokenDates, tokenNumbers) => {
//   console.log("Starting to prepare cache...")

//   let fileCount = 0
//   let loadedFiles = 0
//   let volumes = []
//   let lastVolume = null

//   for (const tokenDate of tokenDates) {
//     for (const tokenNumber of tokenNumbers) {
//       const filePath = `./src/data/ticks/daily-ft-2/combined-tokens/Backtest tokens/CE ATM/All-Tokens/currentTestTokens-6/ticks-${tokenDate}-tk-${tokenNumber}`

//       if (!fs.existsSync(filePath)) {
//         continue
//       }

//       fileCount += 1

//       const data = fs.readFileSync(filePath, "utf8")
//       // const lines = processLines(data)

//       const tickData = JSON.parse(data)

//       // const averageVolumeCache = []
//       // const volume30SecCache = []

//       for (let date in tickData) {
//         const lines = tickData[date]
//         for (let line of lines) {
//           let tick = line

//           // tickCounts = tickCounts + 1

//           const tickTime = new Date(tick.time)

//           // const tickMinute = new Date(
//           //   tickTime.getFullYear(),
//           //   tickTime.getMonth(),
//           //   tickTime.getDate(),
//           //   tickTime.getHours(),
//           //   tickTime.getMinutes()
//           // )

//           let volume = 0
//           if (tick.v !== undefined) {
//             volume = tick.v - (lastVolume || 0)
//             lastVolume = tick.v
//           } else {
//             volume = 0
//           }

//           volumes.push({ time: tickTime, volume_traded: volume })

//           let averageVolume
//           // if (
//           //   averageVolumeCache[
//           //     `${tickTime.getTime()}-${tokenNumber}-${tokenDate}`
//           //   ]
//           // ) {
//           //   averageVolume =
//           //     averageVolumeCache[
//           //       `${tickTime.getTime()}-${tokenNumber}-${tokenDate}`
//           //     ]
//           // } else {
//           // console.log("average volume cached at time", tickTime.getTime())
//           averageVolume = calculateAverageVolume(volumes, tickTime)
//           // Store into averageVolumeCache
//           averageVolumeCache[
//             `${tickTime.getTime()}-${tokenNumber}-${tokenDate}`
//           ] = averageVolume

//           // }

//           // if (
//           //   thirtySecsVolumeCache.hasOwnProperty(
//           //     `${tick.time}-${tokenNumber}-${tokenDate}`
//           //   )
//           // ) {
//           //   thirtySecondVolume =
//           //     thirtySecsVolumeCache[`${tick.time}-${tokenNumber}-${tokenDate}`]
//           // } else {
//           const last30SecondVolumes = volumes.filter(
//             (v) => tickTime - v.time <= 30 * 1000
//           )
//           thirtySecondVolume = last30SecondVolumes.reduce(
//             (total, volume) => total + Number(volume.volume_traded),
//             0
//           )
//           // console.log("30 sec volume cached")
//           // thirtySecsVolumeCache[`${tick.time}-${tokenNumber}-${tokenDate}`] =
//           //   thirtySecondVolume

//           // Store into thirtySecsVolumeCache
//           thirtySecsVolumeCache[
//             `${tickTime.getTime()}-${tokenNumber}-${tokenDate}`
//           ] = thirtySecondVolume
//           // }
//         }
//         volumes = []

//         // let runningVolumeSum = 0

//         // for (let i = 0; i < lines.length; i++) {
//         //   const currentLine = lines[i]
//         //   runningVolumeSum += parseFloat(currentLine.split(",")[1])

//         //   if (i < 30) {
//         //     volume_30Sec.push(runningVolumeSum)
//         //   } else {
//         //     volume_30Sec.push(
//         //       volume_30Sec[i - 30] + parseFloat(currentLine.split(",")[1])
//         //     )
//         //   }

//         //   if (i < 3750) {
//         //     averageVolume.push(runningVolumeSum)
//         //   } else {
//         //     averageVolume.push(
//         //       averageVolume[i - 3750] + parseFloat(currentLine.split(",")[1])
//         //     )
//         //   }
//       }

//       cache[filePath] = tickData

//       loadedFiles += 1
//       console.log(`Cached file: ${loadedFiles} / ${fileCount}`)

//       if (loadedFiles === fileCount) {
//         console.log("Cache prepared.")
//       }
//       // console.log("averageVolumeCache", averageVolumeCache)
//       // console.log("30SecCache", thirtySecsVolumeCache)
//     }
//   }
//   fs.writeFileSync(
//     "./src/data/averageVolumeCache.json",
//     JSON.stringify(averageVolumeCache)
//   )
//   fs.writeFileSync(
//     "./src/data/thirtySecsVolumeCache.json",
//     JSON.stringify(thirtySecsVolumeCache)
//   )
// }

// // const prepareCache = (tokenDates, tokenNumbers) => {
// //   console.log("Starting to prepare cache...")

// //   return new Promise((resolve, reject) => {
// //     let fileCount = 0
// //     let loadedFiles = 0

// //     for (const tokenDate of tokenDates) {
// //       for (const tokenNumber of tokenNumbers) {
// //         const filePath = `./src/data/ticks/daily-ft-2/combined-tokens/Backtest tokens/CE ATM/All-Tokens/currentTestTokens-6/ticks-${tokenDate}-tk-${tokenNumber}`

// //         if (!fs.existsSync(filePath)) {
// //           continue
// //         }

// //         fileCount += 1

// //         fs.readFile(filePath, "utf8", (err, data) => {
// //           if (err) {
// //             reject(err)
// //           } else {
// //             try {
// //               cache[filePath] = JSON.parse(data)
// //               loadedFiles += 1
// //               console.log(`Cached file: ${loadedFiles} / ${fileCount}`)

// //               if (loadedFiles === fileCount) {
// //                 console.log("Cache prepared.")
// //                 resolve()
// //               }
// //             } catch (error) {
// //               reject(error)
// //             }
// //           }
// //         })
// //       }
// //     }
// //   })
// // }

// const init = async () => {
//   // const { tokenDates, tokenNumbers } = await getTokenDatesAndNumbers()

//   if (cluster.isMaster) {
//     console.time("Execution time")

//     getTokenDatesAndNumbers()
//       .then(({ tokenDates, tokenNumbers }) => {
//         // Start preparing the cache before forking worker processes
//         return prepareCache(tokenDates, tokenNumbers).then(() => ({
//           tokenDates,
//           tokenNumbers,
//         }))
//       })
//       .then(({ tokenDates, tokenNumbers }) => {
//         const jobs = prepareJobQueue(
//           tokenDates,
//           tokenNumbers,
//           averageMultipliers,
//           targetPercentageArray,
//           slPercentageArray,
//           trialStopLossPercentageArray
//         )
//         for (let i = 0; i < numCPUs; i++) {
//           const worker = cluster.fork()
//           worker.send(jobs) // send jobs to worker
//         }

//         cluster.on("exit", (worker, code, signal) => {
//           console.log(`Worker ${worker.process.pid} finished`)
//         })
//       })
//   } else {
//     process.on("message", async (jobs) => {
//       const chunkSize = Math.ceil(jobs.length / numCPUs)
//       const workerIndex = cluster.worker.id - 1
//       const jobsChunk = jobs.slice(
//         workerIndex * chunkSize,
//         (workerIndex + 1) * chunkSize
//       )
//       for (const job of jobsChunk) {
//         await backtest(job)
//       }
//       console.log(`Worker ${process.pid} completed its task.`)
//       process.exit(0)
//     })
//   }
// }

// init()
// //         for (let i = 0; i < numCPUs; i++) {
// //           cluster.fork()
// //         }

// //         cluster.on("exit", (worker, code, signal) => {
// //           console.log(`Worker ${worker.process.pid} finished`)
// //         })
// //       })
// //   } else {
// //     const chunkSize = Math.ceil(jobs.length / numCPUs)
// //     const workerIndex = cluster.worker.id - 1
// //     const jobsChunk = jobs.slice(
// //       workerIndex * chunkSize,
// //       (workerIndex + 1) * chunkSize
// //     )
// //     for (const job of jobsChunk) {
// //       await backtest(job)
// //     }
// //     console.log(`Worker ${process.pid} completed its task.`)
// //     process.exit(0)
// //   }
// // }

// // init()
