package utilities

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
)

/**
this function is imported from the standard playground and its aim is to get the external ip address
go to https://play.golang.org/p/BDt3qEQ_2H to have more information
*/
func ExternalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}
	return "", errors.New("are you connected to the network?")
}

type UserInfo struct {
	ID     string
	Topics []string
}

func MapToJson(userIdMap map[string][]string) (outStr *string) {
	var myList []UserInfo
	for key, elem := range userIdMap {
		info := UserInfo{
			ID:     key,
			Topics: elem,
		}

		myList = append(myList, info)
	}

	b, err := json.Marshal(myList)
	if err != nil {
		fmt.Println("error:", err)
	}

	str := string(b)
	fmt.Println(str)
	return &str
}
