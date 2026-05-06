package modules

import (
	"context"
	"slices"

	"github.com/ruffel/brine"
)

// InetAddr is an IPv4 address entry from Salt's network.interfaces response.
type InetAddr struct {
	Address string `json:"address"`
}

// InterfaceInfo is a partial, stable projection of a Salt network interface.
type InterfaceInfo struct {
	HWAddr string     `json:"hwaddr"`
	Up     bool       `json:"up"`
	Inet   []InetAddr `json:"inet"`
}

// Interfaces is the network.interfaces return for one minion.
type Interfaces map[string]InterfaceInfo

// Has reports whether interface name exists.
func (i Interfaces) Has(name string) bool {
	_, ok := i[name]

	return ok
}

// IsUp reports whether interface name exists and is marked up.
func (i Interfaces) IsUp(name string) bool {
	info, ok := i[name]

	return ok && info.Up
}

// IPs returns configured IPv4 addresses on interface name.
func (i Interfaces) IPs(name string) []string {
	info, ok := i[name]
	if !ok {
		return nil
	}

	ips := make([]string, 0, len(info.Inet))
	for _, addr := range info.Inet {
		if addr.Address != "" {
			ips = append(ips, addr.Address)
		}
	}

	return ips
}

// FindByIP returns the first interface containing ip.
func (i Interfaces) FindByIP(ip string) (string, bool) {
	for name, info := range i {
		for _, addr := range info.Inet {
			if addr.Address == ip {
				return name, true
			}
		}
	}

	return "", false
}

// IPAddrs is the network.ip_addrs return for one minion.
type IPAddrs []string

// Has reports whether ip is present.
func (i IPAddrs) Has(ip string) bool { return slices.Contains(i, ip) }

// NetworkInterfaces runs Salt's network.interfaces module.
func NetworkInterfaces(ctx context.Context, client *brine.Client, target brine.Target) (*Result[Interfaces], error) {
	return RunLocal[Interfaces](ctx, client, brine.Local("network.interfaces", target))
}

// NetworkIPAddrs runs Salt's network.ip_addrs module.
func NetworkIPAddrs(ctx context.Context, client *brine.Client, target brine.Target) (*Result[IPAddrs], error) {
	return RunLocal[IPAddrs](ctx, client, brine.Local("network.ip_addrs", target))
}

// NetworkHostnames runs Salt's network.get_hostname module.
func NetworkHostnames(ctx context.Context, client *brine.Client, target brine.Target) (*Result[string], error) {
	return RunLocal[string](ctx, client, brine.Local("network.get_hostname", target))
}
