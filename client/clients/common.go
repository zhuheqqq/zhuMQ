package clients

import "net"

type PartKey struct {
	Name        string `json:"name"`
	Broker_name string `json:"brokername"`
	Broker_H_P  string `json:"brokerhp"`
	Err         string `json:"err"`
}

type BrokerInfo struct {
	Name      string `json:"name"`
	Host_port string `json:"hsotport"`
}

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
