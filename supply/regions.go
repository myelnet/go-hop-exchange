package supply

import (
	"math"
)

// Regions: This is very experimental and needs more iterations to be stable

// RegionCode defines a subnetwork code
type RegionCode uint64

const (
	// GlobalRegion region is a free global network for anyone to try the network
	GlobalRegion RegionCode = iota
	// AsiaRegion is a specific region to connect caches in the Asian area
	AsiaRegion
	// AfricaRegion is a specific region in the African geographic area
	AfricaRegion
	// SouthAmericaRegion is a specific region
	SouthAmericaRegion
	// NorthAmericaRegion is a specific region to connect caches in the North American area
	NorthAmericaRegion
	// EuropeRegion is a specific region to connect caches in the European area
	EuropeRegion
	// OceaniaRegion is a specific region
	OceaniaRegion
	// CustomRegion is a user defined region
	CustomRegion = math.MaxUint64
)

// Region represents a CDN subnetwork
type Region struct {
	Name string
	Code RegionCode
}

var (
	global = Region{
		Name: "Global",
		Code: GlobalRegion,
	}
	asia = Region{
		Name: "Asia",
		Code: AsiaRegion,
	}
	africa = Region{
		Name: "Africa",
		Code: AfricaRegion,
	}
	southAmerica = Region{
		Name: "SouthAmerica",
		Code: SouthAmericaRegion,
	}
	northAmerica = Region{
		Name: "NorthAmerica",
		Code: NorthAmericaRegion,
	}
	europe = Region{
		Name: "Europe",
		Code: EuropeRegion,
	}
	oceania = Region{
		Name: "Oceania",
		Code: OceaniaRegion,
	}
)

// Regions is a list of preset regions
var Regions = map[string]Region{
	"Global":       global,
	"Asia":         asia,
	"Africa":       africa,
	"SouthAmerica": southAmerica,
	"NorthAmerica": northAmerica,
	"Europe":       europe,
	"Oceania":      oceania,
}