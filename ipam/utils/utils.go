package utils

import (
	"errors"
	"net"
	"runtime"

	"github.com/weaveworks/weave/common"
)

var ErrAssertion = errors.New("Assertion Error")

//  We shouldn't ever get any errors on *encoding*, but if we do, this will make sure we get to hear about them.
func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

// IP4int converts an ipv4 address to a uint32
func IP4int(ip4 net.IP) (r uint32) {
	for _, b := range ip4.To4() {
		r <<= 8
		r |= uint32(b)
	}
	return
}

// IntIP4 converts a uint32 to an ipv4 address
func IntIP4(key uint32) (r net.IP) {
	r = make([]byte, net.IPv4len)
	for i := 3; i >= 0; i-- {
		r[i] = byte(key)
		key >>= 8
	}
	return
}

// Add - convert to 32-bit unsigned integer, add, and convert back
func Add(addr net.IP, i uint32) net.IP {
	sum := IP4int(addr) + i
	return IntIP4(sum)
}

// Subtract - convert to 32-bit unsigned integer, subtract, and convert back
func Subtract(a, b net.IP) int64 {
	return int64(IP4int(a)) - int64(IP4int(b))
}

// Assert test is true, panic otherwise
func Assert(test bool) {
	if !test {
		var buf = make([]byte, 1024)
		written := runtime.Stack(buf, false)
		common.Error.Printf("Assertion Error:\n%s", buf[:written])
		panic(ErrAssertion)
	}
}
