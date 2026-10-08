package api

import "github.com/Harvey-Mei/BridgeOps/backend/usertime"

func configuredUserTime(d *Deps) *usertime.Service {
	if d != nil && d.UserTime != nil {
		return d.UserTime
	}
	return usertime.Default()
}
