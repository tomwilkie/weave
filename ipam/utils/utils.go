package ip

import (
	"net"
)

// Using 32-bit integer to represent IPv4 address
type Address uint32
type Offset uint32

type Range struct {
	Start, End Address // [Start, End); Start <= End
}

func ParseIP(s string) Address {
	return IP4Address(net.ParseIP(s))
}

// IP4Address converts an ipv4 address to our integer address type
func IP4Address(ip4 net.IP) (r Address) {
	for _, b := range ip4.To4() {
		r <<= 8
		r |= Address(b)
	}
	return
}

// AddressIP4 converts our integer address type to an ipv4 address
func AddressIP4(key Address) (r net.IP) {
	r = make([]byte, net.IPv4len)
	for i := 3; i >= 0; i-- {
		r[i] = byte(key)
		key >>= 8
	}
	return
}

func (addr Address) String() string {
	return AddressIP4(addr).String()
}

func Add(addr Address, i Offset) Address {
	return addr + Address(i)
}

func Subtract(a, b Address) Offset {
	Assert(a >= b)
	return Offset(a - b)
}

// Assert test is true, panic otherwise
func Assert(test bool) {
	if !test {
		panic("Assertion failure")
	}
}
