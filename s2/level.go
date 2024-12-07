package s2

/*
https://s2geometry.io/resources/s2cell_statistics.html

S2 Cell Statistics
> The average size is that returned by S2Cell.AverageArea(), guaranteed to be within a factor of 1.5 of the high and low end of the range.

level  min area     max area     average area  units  Random cell 1 (UK) min edge length  Random cell 1 (UK) max edge length  Random cell 2 (US) min edge length  Random cell 2 (US) max edge length  Number of cells

00     85011012.19  85011012.19  85011012.19   km2    7842 km                           7842 km                           7842 km                           7842 km                           6
01     21252753.05  21252753.05  21252753.05   km2    3921 km                           5004 km                           3921 km                           5004 km                           24
02     4919708.23   6026521.16   5313188.26    km2    1825 km                           2489 km                           1825 km                           2489 km                           96
03     1055377.48   1646455.50   1328297.07    km2    840 km                            1167 km                           1130 km                           1310 km                           384
04     231564.06    413918.15    332074.27     km2    432 km                            609 km                            579 km                            636 km                            1536
05     53798.67     104297.91    83018.57      km2    210 km                            298 km                            287 km                            315 km                            6K - continental sized
06     12948.81     26113.30     20754.64      km2    108 km                            151 km                            143 km                            156 km                            24K
07     3175.44      6529.09      5188.66       km2    54 km                             76 km                             72 km                             78 km                             98K
08     786.20       1632.45      1297.17       km2    27 km                             38 km                             36 km                             39 km                             393K - about a day's walk/ride
09     195.59       408.12       324.29        km2    14 km                             19 km                             18 km                             20 km                             1573K
10     48.78        102.03       81.07         km2    7 km                              9 km                              9 km                              10 km                            6M
11     12.18        25.51        20.27         km2    3 km                              5 km                              4 km                              5 km                             25M
12     3.04         6.38         5.07          km2    1699 m                            2 km                              2 km                              2 km                             100M
13     0.76         1.59         1.27          km2    850 m                             1185 m                            1123 m                            1225 m                           402M -- about a kilometer (square)
14     0.19         0.40         0.32          km2    425 m                             593 m                             562 m                             613 m                            1610M
15     47520.30     99638.93     79172.67      m2     212 m                             296 m                             281 m                             306 m                            6B
16     11880.08     24909.73     19793.17      m2     106 m                             148 m                             140 m                             153 m                            25B -- 100m-180m (_141m) / 20,000 m^2 =~ 0.02km^2 =~ 0.0078miles^2 throwing distance
17     2970.02      6227.43      4948.29       m2     53 m                              74 m                              70 m                              77 m                             103B --
18     742.50       1556.86      1237.07       m2     27 m                              37 m                              35 m                              38 m                             412B
19     185.63       389.21       309.27        m2     13 m                              19 m                              18 m                              19 m                             1649B
20     46.41        97.30        77.32         m2     7 m                               9 m                               9 m                               10 m                            7T
21     11.60        24.33        19.33         m2     3 m                               5 m                               4 m                               5 m                             26T - spitting distance
22     2.90         6.08         4.83          m2     166 cm                            2 m                               2 m                               2 m                             105T
23     0.73         1.52         1.21          m2     83 cm                             116 cm                            110 cm                            120 cm                           422T -- (_1.1m) / 1.21m^2 a city of broad shoulders
24     0.18         0.38         0.30          m2     41 cm                             58 cm                             55 cm                             60 cm                            1689T
25     453.19       950.23       755.05        cm2    21 cm                             29 cm                             27 cm                             30 cm                            7e15
26     113.30       237.56       188.76        cm2    10 cm                             14 cm                             14 cm                             15 cm                            27e15
27     28.32        59.39        47.19         cm2    5 cm                              7 cm                              7 cm                              7 cm                             108e15
28     7.08         14.85        11.80         cm2    2 cm                              4 cm                              3 cm                              4 cm                             432e15
29     1.77         3.71         2.95          cm2    12 mm                             18 mm                             17 mm                             18 mm                            1729e15
30     0.44         0.93         0.74          cm2    6 mm                              9 mm                              8 mm                              9 mm                             7e18
*/

// CellLevel represents the S2 cell level, from 0-30.
type CellLevel int

const (
	// CellLevel0 covers earth in 6 cells.
	CellLevel0 CellLevel = 0

	CellLevel1 CellLevel = 1
	CellLevel2 CellLevel = 2
	CellLevel3 CellLevel = 3
	CellLevel4 CellLevel = 4
	CellLevel5 CellLevel = 5

	// CellLevel6 is wider than the Idaho panhandle.
	// Around size of Massachusetts?
	CellLevel6  CellLevel = 6
	CellLevel7  CellLevel = 7
	CellLevel8  CellLevel = 8
	CellLevel9  CellLevel = 9
	CellLevel10 CellLevel = 10
	CellLevel11 CellLevel = 11
	CellLevel12 CellLevel = 12

	// CellLevel13 is about a 1/2 section.
	CellLevel13 CellLevel = 13

	// CellLevel14 is about 80 acres.
	CellLevel14 CellLevel = 14

	// CellLevel15 is about 20 acres.
	CellLevel15 CellLevel = 15

	// CellLevel16 is approximately 140m on an edge, or an area of about 5 acres.
	CellLevel16 CellLevel = 16

	// CellLevel17 has an area of about 1.2 acres.
	CellLevel17 CellLevel = 17

	// CellLevel18 is about 100ft on a side, and has an area of about 1/4 acre.
	// Small residential plot.
	CellLevel18 CellLevel = 18
	CellLevel19 CellLevel = 19
	CellLevel20 CellLevel = 20
	CellLevel21 CellLevel = 21
	CellLevel22 CellLevel = 22

	// CellLevel23 is approximately a human body; 1 square meter.
	CellLevel23 CellLevel = 23
	CellLevel24 CellLevel = 24
	CellLevel25 CellLevel = 25
	CellLevel26 CellLevel = 26
	CellLevel27 CellLevel = 27
	CellLevel28 CellLevel = 28
	CellLevel29 CellLevel = 29
	CellLevel30 CellLevel = 30
)
