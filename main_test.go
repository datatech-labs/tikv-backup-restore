package main

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
)

func Test_main(t *testing.T) {
	t.Run("validate one key", func(t *testing.T) {
		db := dbTxn{}
		db.connect("pd0:2379")
		tx, err := db.client.Begin(context.TODO())
		if err != nil {
			t.Error(err)
		}
		key := []byte("TEST-KEY")
		value := []byte("TEST-VALUE-1")
		if err := tx.Set(key, value); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(context.TODO()); err != nil {
			t.Fatal(err)
		}
		if err := db.backup("/tmp/backup.tar.gz"); err != nil {
			t.Fatal(err)
		}
		tx, err = db.client.Begin(context.TODO())
		if err != nil {
			t.Error(err)
		}
		if err := tx.Delete(key); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(context.TODO()); err != nil {
			t.Fatal(err)
		}
		if err := db.restore("/tmp/backup.tar.gz"); err != nil {
			t.Fatal(err)
		}
		tx, err = db.client.Begin(context.TODO())
		if err != nil {
			t.Error(err)
		}
		v, err := tx.Get(context.TODO(), key)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(context.TODO()); err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(v, value) != 0 {
			t.Fatalf("invalid value")
		}
	})
	t.Run("validate backup", func(t *testing.T) {
		dbClient := dbTxn{}
		dbClient.connect("pd0:2379")
		if err := dbClient.connect("pd0:2379"); err != nil {
			t.Fatal(err)
		}
		if err := randomfill(&dbClient, 32768); err != nil {
			t.Fatal(err)
		}
		if err := dbClient.backup("/tmp/backup.tar.gz"); err != nil {
			t.Fatal(err)
		}
		if err := dbClient.validate("/tmp/backup.tar.gz"); err != nil {
			t.Fatal(err)
		}
	})
}

func randomfill(dbtxn *dbTxn, amount int) error {
	tx, err := dbtxn.client.Begin(context.TODO())
	if err != nil {
		return err
	}
	for idx := 0; idx < amount; idx++ {
		key := []byte(fmt.Sprintf("key%d", idx))
		if err := tx.Set(key, randomBlock()); err != nil {
			return err
		}
	}
	return tx.Commit(context.TODO())
}

func randomBlock() []byte {
	size := rand.Intn(512) + 2048
	block := make([]byte, size)
	for idx := 0; idx < size; idx++ {
		block[idx] = byte(rand.Intn(256))
	}
	return block
}
