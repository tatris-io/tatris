package consts

import (
	"fmt"
	"strings"
)

var (
	// revision, revisionDate and buildTime are assigned in Makefile, do not edit them manually
	revision     = "undefined"
	revisionDate = "undefined"
	buildTime    = "undefined"

	ver = Semver{
		major:  0,
		minor:  1,
		patch:  0,
		extTag: "alpha",
	}
)

type Semver struct {
	major, minor, patch uint64
	extTag              string
}

func Version() string {
	extTag := ver.extTag
	if extTag != "" {
		extTag = "-" + extTag
	}

	version := fmt.Sprintf("tatris version: %d.%d.%d%s\n", ver.major, ver.minor, ver.patch, extTag)

	build := fmt.Sprintf("detail revision: %s-%s\n", revisionDate, revision)
	if strings.Contains(build, "undefined") {
		build = "unknown"
	}

	result := version + build
	if buildTime != "undefined" {
		result = result + fmt.Sprintf("build time: %s", buildTime)
	}
	return result
}
