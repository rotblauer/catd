package common

/*
https://en.wikipedia.org/wiki/Decimal_degrees?useskin=vector

Degree precision versus length decimal
places 	decimal
degrees 	DMS 	Object that can be unambiguously recognized at this scale 	N/S or E/W
at equator 	E/W at
23N/S 	E/W at
45N/S 	E/W at
67N/S
0 	1.0 	1° 00′ 0″ 	country or large region 	111 km 	102 km 	78.7 km 	43.5 km
1 	0.1 	0° 06′ 0″ 	large city or district 	11.1 km 	10.2 km 	7.87 km 	4.35 km
2 	0.01 	0° 00′ 36″ 	town or village 	1.11 km 	1.02 km 	0.787 km 	0.435 km
3 	0.001 	0° 00′ 3.6″ 	neighborhood, street 	111 m 	102 m 	78.7 m 	43.5 m
4 	0.0001 	0° 00′ 0.36″ 	individual street, large buildings 	11.1 m 	10.2 m 	7.87 m 	4.35 m
5 	0.00001 	0° 00′ 0.036″ 	individual trees, houses 	1.11 m 	1.02 m 	0.787 m 	0.435 m
6 	0.000001 	0° 00′ 0.0036″ 	individual cats 	111 mm 	102 mm 	78.7 mm 	43.5 mm
7 	0.0000001 	0° 00′ 0.00036″ 	practical limit of commercial surveying 	11.1 mm 	10.2 mm 	7.87 mm 	4.35 mm
8 	0.00000001 	0° 00′ 0.000036″ 	specialized surveying 	1.11 mm 	1.02 mm 	0.787 mm 	0.435 mm
*/

const (
	// GPSPrecision0 is the precision for country or large region
	GPSPrecision0 = 0
	// GPSPrecision1 is the precision for large city or district
	GPSPrecision1 = 1
	// GPSPrecision2 is the precision for town or village
	GPSPrecision2 = 2
	// GPSPrecision3 is the precision for neighborhood, street
	GPSPrecision3 = 3
	// GPSPrecision4 is the precision for individual street, large buildings
	GPSPrecision4 = 4
	// GPSPrecision5 is the precision for individual trees, houses
	GPSPrecision5 = 5
	// GPSPrecision6 is the precision for individual cats
	GPSPrecision6 = 6
	// GPSPrecision7 is the precision for practical limit of commercial surveying
	GPSPrecision7 = 7
	// GPSPrecision8 is the precision for specialized surveying
	GPSPrecision8 = 8
)
