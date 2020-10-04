package main

import (
	"flag"
	"os"

	"github.com/sirupsen/logrus"
)

func main() {
	flagMode := flag.String("mode", "raw", "raw or txn")
	flagAddr := flag.String("addr", "pd0:2379", "pd address")
	flag.Parse()

	if *flagAddr == "" {
		flag.PrintDefaults()
		os.Exit(-1)
	}

	var dbClient db
	switch *flagMode {
	case "raw":
		dbClient = newDBRaw()
	case "txn":
		dbClient = newDBTxn()
	default:
		flag.PrintDefaults()
		os.Exit(-1)
	}

	if err := dbClient.connect(*flagAddr); err != nil {
		logrus.Fatal(err)
	}
}
