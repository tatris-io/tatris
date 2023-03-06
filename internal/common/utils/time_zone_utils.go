// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package utils

import "time"

func ParseTimeZone(timeZoneStr string) *time.Location {
	var loc *time.Location
	if len(timeZoneStr) > 0 {
		if timeZoneStr[0] == '+' || timeZoneStr[0] == '-' {
			if len(timeZoneStr) == 6 && timeZoneStr[3] == ':' {
				// -01:00
				// When parsing a time with a zone offset like -0700, if the offset corresponds to a
				// time zone used by the current location (Local), then Parse uses that location and
				// zone in the returned time. Otherwise it records the time as being in a fabricated
				// location with time
				// fixed at the given zone offset.
				tt, _ := time.Parse("-07:00", timeZoneStr)
				loc = tt.Location()
			} else if len(timeZoneStr) == 5 {
				// -0100
				tt, _ := time.Parse("-0700", timeZoneStr)
				loc = tt.Location()
			}
		} else {
			loc, _ = time.LoadLocation(timeZoneStr)
		}
	}

	return loc
}
