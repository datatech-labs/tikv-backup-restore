package main

import (
	"context"

	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	"github.com/tikv/client-go/txnkv"
)

type db interface {
	connect(addr string) error
}

type dbTxn struct {
	client *txnkv.Client
}

func newDBTxn() db {
	db := dbTxn{}
	return &db
}

func (dbtxn *dbTxn) connect(addr string) error {
	client, err := txnkv.NewClient(context.TODO(), []string{addr}, config.Default())
	if err != nil {
		return err
	}
	dbtxn.client = client
	return nil
}

type dbRaw struct {
	client *rawkv.Client
}

func newDBRaw() db {
	db := dbRaw{}
	return &db
}

func (dbraw *dbRaw) connect(addr string) error {
	client, err := rawkv.NewClient(context.TODO(), []string{addr}, config.Default())
	if err != nil {
		return err
	}
	dbraw.client = client
	return nil
}

func (dbraw *dbRaw) put(key, value []byte) error {
	return dbraw.client.Put(key, value)
}
