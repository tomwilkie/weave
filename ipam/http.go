package ipam

import (
	"errors"
	"fmt"
	. "github.com/zettio/weave/common"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
)

// Parse a URL of the form /xxx/<identifier>
func parseUrl(url string) (identifier string, err error) {
	parts := strings.Split(url, "/")
	if len(parts) != 3 {
		return "", errors.New("Unable to parse url: " + url)
	}
	return parts[2], nil
}

// Parse a URL of the form /xxx/<identifier>/<ip-address>
func parseUrlWithIP(url string) (identifier string, ipaddr string, err error) {
	parts := strings.Split(url, "/")
	if len(parts) != 4 {
		return "", "", errors.New("Unable to parse url: " + url)
	}
	return parts[2], parts[3], nil
}

func httpErrorAndLog(level *log.Logger, w http.ResponseWriter, msg string,
	status int, logmsg string, logargs ...interface{}) {
	http.Error(w, msg, status)
	level.Printf(logmsg, logargs...)
}

func (alloc *Allocator) HandleHttp(mux *http.ServeMux) {
	mux.HandleFunc("/ip/", func(w http.ResponseWriter, r *http.Request) {
		var closedChan = w.(http.CloseNotifier).CloseNotify()

		switch r.Method {
		case "GET": // caller requests one address for a container
			ident, err := parseUrl(r.URL.Path)
			if err != nil {
				httpErrorAndLog(Warning, w, "Invalid request", http.StatusBadRequest, err.Error())
			} else if newAddr := alloc.GetFor(ident, closedChan); newAddr != nil {
				io.WriteString(w, fmt.Sprintf("%s/%d", newAddr, alloc.universeLen))
			} else {
				httpErrorAndLog(
					Error, w, "No free addresses", http.StatusServiceUnavailable,
					"No free addresses")
			}
		case "DELETE": // opposite of PUT for one specific address or all addresses
			ident, ipStr, err := parseUrlWithIP(r.URL.Path)
			if err != nil {
				httpErrorAndLog(Warning, w, "Invalid request", http.StatusBadRequest, err.Error())
			} else if ipStr == "*" {
				alloc.DeleteRecordsFor(ident)
			} else if ip := net.ParseIP(ipStr); ip == nil {
				httpErrorAndLog(Warning, w, "Invalid IP", http.StatusBadRequest,
					"Invalid IP in request: %s", ipStr)
				return
			} else if err = alloc.Free(ident, ip); err != nil {
				httpErrorAndLog(Warning, w, "Invalid Free", http.StatusBadRequest, err.Error())
			}
		default:
			http.Error(w, "Verb not handled", http.StatusBadRequest)
		}
	})
}

func ListenHttp(port int, alloc *Allocator) {
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, fmt.Sprintln(alloc))
	})
	alloc.HandleHttp(mux)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}
	if err := srv.ListenAndServe(); err != nil {
		Error.Fatal("Unable to create http listener: ", err)
	}
}
