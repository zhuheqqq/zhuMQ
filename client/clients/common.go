package clients

import "net"

func GetIpport() string {
	interfaces, err := net.Interfaces()
	ipport := ""
	if err != nil {
		panic("Poor soul, here is what you got:" + err.Error())
	}
	for _, inter := range interfaces {
		mac := inter.HardwareAddr
		ipport += mac.String()
	}
	return ipport
}
