package common

// All units are in metric:
// - Speed is in m/s
// - Distance is in meters
// - Time is in seconds
// - Acceleration is in m/s^2
// - Pressure is in pascals
// - Temperature is in kelvin... wait? what?

const SpeedOfWalkingMin = 0.42              // or 1.5 km/h or 1 mph
const SpeedOfWalkingMax = 1.78              // or 6.4 km/h or 4 mph
const SpeedOfWalkingMean = 1.2              // or 4.3 km/h or 2.7 mph
const SpeedOfRunningMin = 2.23              // or 8 km/h or 5 mph
const SpeedOfRunningMax = 5.56              // or 12 mph or 20 km/h
const SpeedOfCyclingMin = SpeedOfRunningMin // or 25 km/h
const SpeedOfCyclingMax = 11.76             // or 42 km/h or 26 mph
const SpeedOfDrivingMin = 6.7               // 15 mph or 24 km/h or 6.7 m/s
const SpeedOfCityDriving = 13.9             // or 50 km/h
const SpeedOfHighwayDriving = 24.58         // or 88 km/h or 55 mph
const SpeedOfCommercialFlight = 250.0       // or 900 km/h
const SpeedOfSound = 343.0

const ElevationOfEverest = 8848.0
const ElevationCommercialFlight = 10668.0
const ElevationOfTroposphere = 11000.0
const ElevationOfDeadSea = -430.0
