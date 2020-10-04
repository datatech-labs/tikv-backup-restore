package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/key"
	"github.com/tikv/client-go/txnkv"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	constNodeSize = 4096
)

type keysValues struct {
	Keys   [][]byte
	Values [][]byte
}

type db interface {
	connect(addr string) error
	backup(outfile string) error
	restore(infile string) error
	validate(infile string) error
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

func (dbtxn *dbTxn) backup(outfile string) error {
	file, err := os.Create(outfile)
	if err != nil {
		return err
	}
	gw := gzip.NewWriter(file)
	tw := tar.NewWriter(gw)
	filenameIndex := 0
	tx, err := dbtxn.client.Begin(context.TODO())
	if err != nil {
		return err
	}
	it, err := tx.Iter(context.TODO(), nil, nil)
	keys := [][]byte{}
	values := [][]byte{}
	for it.Valid() {
		keys = append(keys, it.Key()[:])
		values = append(values, it.Value()[:])
		if len(keys) >= constNodeSize {
			if err := writeToTar(tw, filenameIndex, keys, values); err != nil {
				return err
			}
			filenameIndex++
			keys = [][]byte{}
			values = [][]byte{}
		}
		if err := it.Next(context.TODO()); err != nil {
			return err
		}
	}
	it.Close()
	if len(keys) >= 0 {
		if err := writeToTar(tw, filenameIndex, keys, values); err != nil {
			return err
		}
	}
	if err := tx.Commit(context.TODO()); err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	if err := gw.Close(); err != nil {
		return err
	}
	return file.Close()
}

func (dbtxn *dbTxn) restore(infile string) error {
	file, err := os.Open(infile)
	if err != nil {
		return err
	}
	gr, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	tx, err := dbtxn.client.Begin(context.TODO())
	if err != nil {
		return err
	}
	tr := tar.NewReader(gr)
	b := bytes.Buffer{}
	for {
		_, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		b.Reset()
		if _, err := b.ReadFrom(tr); err != nil {
			return err
		}
		kv := keysValues{}
		if err := bson.Unmarshal(b.Bytes(), &kv); err != nil {
			return err
		}
		for idx, key := range kv.Keys {
			if err := tx.Set(key, kv.Values[idx]); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(context.TODO()); err != nil {
		return err
	}
	if err := gr.Close(); err != nil {
		return err
	}
	return file.Close()
}

func (dbtxn *dbTxn) validate(infile string) error {
	file, err := os.Open(infile)
	if err != nil {
		return err
	}
	gr, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	tx, err := dbtxn.client.Begin(context.TODO())
	if err != nil {
		return err
	}
	tr := tar.NewReader(gr)
	b := bytes.Buffer{}
	for {
		_, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		b.Reset()
		if _, err := b.ReadFrom(tr); err != nil {
			return err
		}
		kv := keysValues{}
		if err := bson.Unmarshal(b.Bytes(), &kv); err != nil {
			return err
		}
		keys := []key.Key{}
		for _, k := range kv.Keys {
			keys = append(keys, key.Key(k))
		}
		result, err := tx.BatchGet(context.TODO(), keys)
		if err != nil {
			return err
		}
		for idx, k := range kv.Keys {
			v, ok := result[string(k)]
			if !ok {
				return fmt.Errorf("key {%s} not found in database", string(k))
			}
			if v == nil {
				return fmt.Errorf("key {%s} have empty value in database", string(k))
			}
			if bytes.Compare(v, kv.Values[idx]) != 0 {
				return fmt.Errorf("key {%s} have invalid value", string(k))
			}
		}
	}
	if err := tx.Commit(context.TODO()); err != nil {
		return err
	}
	if err := gr.Close(); err != nil {
		return err
	}
	return file.Close()
}

func writeToTar(tw *tar.Writer, filenameIndex int, keys, values [][]byte) error {
	kv := keysValues{
		Keys:   keys,
		Values: values,
	}
	data, err := bson.Marshal(&kv)
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{
		Name:    fmt.Sprintf("node%d", filenameIndex),
		Size:    int64(len(data)),
		Mode:    0600,
		ModTime: time.Now(),
	}); err != nil {
		return err
	}
	if _, err := tw.Write(data); err != nil {
		return err
	}
	return nil
}
