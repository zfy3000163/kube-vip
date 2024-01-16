package manager

import (
	"testing"

	"github.com/kube-vip/kube-vip/pkg/kubevip"
)

func TestCleanLbIp(t *testing.T) {

	tests := []struct {
		name    string
		ip      string
		wantErr bool
		c       *kubevip.Config
	}{
		{"", "192.168.0.1", false, &kubevip.Config{Interface: "lo", ServicesInterface: "lo"}},
		{"", "10.121.219.12", false, &kubevip.Config{Interface: "lo", ServicesInterface: "lo"}},
		//{"", &Config{Interface: "eth0", ServicesInterface: "eth1"}, false},
	}

	for _, tt := range tests {
		t.Logf("%v", tt.ip)
		t.Run(tt.name, func(t *testing.T) {
			if err := cleanLbIp(tt.ip, tt.c); (err != false) != tt.wantErr {
				t.Errorf("cleanLbIp() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
