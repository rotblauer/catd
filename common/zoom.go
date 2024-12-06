package common

/*
Level 	# Tiles 	Tile width
(° of longitudes) 	m / pixel
(on Equator) 	~ Scale
(on screen) 	Examples of
areas to represent
0 	1 	360 	156 543 	1:500 million 	whole world
1 	4 	180 	78 272 	1:250 million
2 	16 	90 	39 136 	1:150 million 	subcontinental area
3 	64 	45 	19 568 	1:70 million 	largest country
4 	256 	22.5 	9 784 	1:35 million
5 	1 024 	11.25 	4 892 	1:15 million 	large African country
6 	4 096 	5.625 	2 446 	1:10 million 	large European country
7 	16 384 	2.813 	1 223 	1:4 million 	small country, US state
8 	65 536 	1.406 	611.496 	1:2 million
9 	262 144 	0.703 	305.748 	1:1 million 	wide area, large metropolitan area
10 	1 048 576 	0.352 	152.874 	1:500 thousand 	metropolitan area
11 	4 194 304 	0.176 	76.437 	1:250 thousand 	city
12 	16 777 216 	0.088 	38.219 	1:150 thousand 	town, or city district
13 	67 108 864 	0.044 	19.109 	1:70 thousand 	village, or suburb
14 	268 435 456 	0.022 	9.555 	1:35 thousand
15 	1 073 741 824 	0.011 	4.777 	1:15 thousand 	small road
16 	4 294 967 296 	0.005 	2.389 	1:8 thousand 	street
17 	17 179 869 184 	0.003 	1.194 	1:4 thousand 	block, park, addresses
18 	68 719 476 736 	0.001 	0.597 	1:2 thousand 	some buildings, trees
19 	274 877 906 944 	0.0005 	0.299 	1:1 thousand 	local highway and crossing details
20 	1 099 511 627 776 	0.00025 	0.149 	1:5 hundred 	A mid-sized building
*/

type SlippyZoomLevelT int

var (
	// SlippyZoomLevel0 represents, eg. the whole world
	SlippyZoomLevel0 SlippyZoomLevelT = 0
	SlippyZoomLevel1 SlippyZoomLevelT = 1

	// SlippyZoomLevel2 represents, eg. a subcontinental area
	SlippyZoomLevel2 SlippyZoomLevelT = 2

	// SlippyZoomLevel3 represents, eg. the largest country
	SlippyZoomLevel3 SlippyZoomLevelT = 3
	SlippyZoomLevel4 SlippyZoomLevelT = 4

	// SlippyZoomLevel5 represents, eg. a large African country
	SlippyZoomLevel5 SlippyZoomLevelT = 5
	// SlippyZoomLevel6 represents, eg. a large European country
	SlippyZoomLevel6 SlippyZoomLevelT = 6
	// SlippyZoomLevel7 represents, eg. a small country, US state
	SlippyZoomLevel7 SlippyZoomLevelT = 7
	SlippyZoomLevel8 SlippyZoomLevelT = 8
	// SlippyZoomLevel9 represents, eg. a wide area, large metropolitan area
	SlippyZoomLevel9 SlippyZoomLevelT = 9
	// SlippyZoomLevel10 represents, eg. a metropolitan area
	SlippyZoomLevel10 SlippyZoomLevelT = 10
	// SlippyZoomLevel11 represents, eg. a city
	SlippyZoomLevel11 SlippyZoomLevelT = 11
	// SlippyZoomLevel12 represents, eg. a town, or city district
	SlippyZoomLevel12 SlippyZoomLevelT = 12
	// SlippyZoomLevel13 represents, eg. a village, or suburb
	SlippyZoomLevel13 SlippyZoomLevelT = 13
	SlippyZoomLevel14 SlippyZoomLevelT = 14
	// SlippyZoomLevel15 represents, eg. a small road
	SlippyZoomLevel15 SlippyZoomLevelT = 15
	// SlippyZoomLevel16 represents, eg. a street
	SlippyZoomLevel16 SlippyZoomLevelT = 16
	// SlippyZoomLevel17 represents, eg. a block, park, addresses
	SlippyZoomLevel17 SlippyZoomLevelT = 17
	// SlippyZoomLevel18 represents, eg. some buildings, trees
	SlippyZoomLevel18 SlippyZoomLevelT = 18
	// SlippyZoomLevel19 represents, eg. local highway and crossing details
	SlippyZoomLevel19 SlippyZoomLevelT = 19
	// SlippyZoomLevel20 represents, eg. a mid-sized building
	SlippyZoomLevel20 SlippyZoomLevelT = 20
)
