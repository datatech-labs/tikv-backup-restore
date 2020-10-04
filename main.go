package main

import (
	"flag"
	"math/rand"
	"time"

	"github.com/sirupsen/logrus"
)

func main() {
	flagAddr := flag.String("addr", "127.0.0.1:2379", "pd address")
	flagOutputFile := flag.String("outfile", "backup.tar.gz", "file to backup")
	flagMode := flag.String("mode", "backup", "backup or restore")
	flag.Parse()

	dbClient := newDBTxn()

	rand.Seed(time.Now().Unix())

	if err := dbClient.connect(*flagAddr); err != nil {
		logrus.Fatal(err)
	}

	switch *flagMode {
	case "backup":
		if err := dbClient.backup(*flagOutputFile); err != nil {
			logrus.Fatal(err)
		}
	case "restore":
		logrus.Fatal("restore unsupported")
	default:
		logrus.Fatalf("unknown mode %s\n", *flagMode)
	}
}
