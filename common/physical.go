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
const SpeedOfDrivingCityUSMean = 13.9       // or 50 km/h
const SpeedOfDrivingHighwayMin = 20.11      // or 72 km/h or 45 mph
const SpeedOfDrivingHighway = 25.29         // or 91 km/h or 56 mph
const SpeedOfDrivingFreeway = 33.33         // or 120 km/h or 75 mph
const SpeedOfDrivingAutobahn = 67.06        // or 241 km/h or 150 mph
const SpeedOfCommercialFlight = 250.0       // or 900 km/h
const SpeedOfSound = 343.0

const ElevationOfEverest = 8848.0
const ElevationCommercialFlightMean = 10668.0
const ElevationOfTroposphere = 11000.0
const ElevationOfDeadSea = -430.0
