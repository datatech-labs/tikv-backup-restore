package main

import (
	"context"
	"flag"

	"github.com/sirupsen/logrus"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/txnkv"
)

func main() {
	flagAddr := flag.String("addr", "pd0:2379", "pd address")
	flag.Parse()

	if *flagAddr == "" {
		flag.PrintDefaults()
	}

	if err := backup(*flagAddr); err != nil {
		logrus.Error(err)
	}
}

func backup(addr string) error {
	_, err := connect(addr)
	return err
}

func connect(addr string) (*txnkv.Client, error) {
	return txnkv.NewClient(context.TODO(), []string{addr}, config.Default())
}
