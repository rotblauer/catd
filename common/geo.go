package common

import "github.com/paulmach/orb"

// SegmentsIntersect returns true if the two line segments intersect
// and the intersection point, otherwise false and nils.
// The intersection point is considered exclusive of the endpoints of the segments;
// continuous segments are not considered to intersect.
/*
	https://stackoverflow.com/a/1968345

	// Returns 1 if the lines intersect, otherwise 0. In addition, if the lines
	// intersect the intersection point may be stored in the floats i_x and i_y.
	char get_line_intersection(float p0_x, float p0_y, float p1_x, float p1_y,
	    float p2_x, float p2_y, float p3_x, float p3_y, float *i_x, float *i_y)
	{
	    float s1_x, s1_y, s2_x, s2_y;
	    s1_x = p1_x - p0_x;     s1_y = p1_y - p0_y;
	    s2_x = p3_x - p2_x;     s2_y = p3_y - p2_y;

	    float s, t;
	    s = (-s1_y * (p0_x - p2_x) + s1_x * (p0_y - p2_y)) / (-s2_x * s1_y + s1_x * s2_y);
	    t = ( s2_x * (p0_y - p2_y) - s2_y * (p0_x - p2_x)) / (-s2_x * s1_y + s1_x * s2_y);

	    if (s >= 0 && s <= 1 && t >= 0 && t <= 1)
	    {
	        // Collision detected
	        if (i_x != NULL)
	            *i_x = p0_x + (t * s1_x);
	        if (i_y != NULL)
	            *i_y = p0_y + (t * s1_y);
	        return 1;
	    }

	    return 0; // No collision
	}
*/
func SegmentsIntersect(segA, segB orb.LineString) (intersect bool, x, y *float64) {
	p0_x, p0_y := segA[0][0], segA[0][1]
	p1_x, p1_y := segA[1][0], segA[1][1]
	p2_x, p2_y := segB[0][0], segB[0][1]
	p3_x, p3_y := segB[1][0], segB[1][1]
	var s1_x, s1_y, s2_x, s2_y float64
	s1_x = p1_x - p0_x
	s1_y = p1_y - p0_y
	s2_x = p3_x - p2_x
	s2_y = p3_y - p2_y
	var s, t float64
	s = (-s1_y*(p0_x-p2_x) + s1_x*(p0_y-p2_y)) / (-s2_x*s1_y + s1_x*s2_y)
	t = (s2_x*(p0_y-p2_y) - s2_y*(p0_x-p2_x)) / (-s2_x*s1_y + s1_x*s2_y)
	if s >= 0 && s <= 1 && t >= 0 && t <= 1 {
		i_x := p0_x + (t * s1_x)
		i_y := p0_y + (t * s1_y)
		i := orb.Point{i_x, i_y}

		// Intersection is considered/defined EXCLUSIVE of the endpoints of a (either) segment,
		// i.e. continuous segments are not considered to intersect.
		if !segA.Bound().Min.Equal(i) && !segA.Bound().Max.Equal(i) {
			return true, &i_x, &i_y
		}
	}
	return false, nil, nil
}
