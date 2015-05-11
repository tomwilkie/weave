package utils

import (
	"net"
)

// Using 32-bit integer to represent IPv4 address
type Address uint32
type Offset uint32

// We shouldn't ever get any errors on *encoding*, but if we do, this will make sure we get to hear about them.
func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
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

// Add - convert to 32-bit unsigned integer, add, and convert back
func Add(addr net.IP, i Offset) net.IP {
	sum := IP4Address(addr) + Address(i)
	return AddressIP4(sum)
}

// Subtract - convert to 32-bit unsigned integer, subtract b from a
func Subtract(a, b net.IP) Offset {
	Assert(GE(a, b))
	return Offset(IP4Address(a) - IP4Address(b))
}

// GE - return true if a is greater than or equal to b
func GE(a, b net.IP) bool {
	return IP4Address(a) >= IP4Address(b)
}

// Assert test is true, panic otherwise
func Assert(test bool) {
	if !test {
		panic("Assertion failure")
	}
}
