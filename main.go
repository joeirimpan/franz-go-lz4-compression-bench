package main

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"sync"

	"github.com/pierrec/lz4"
)

type decompressor struct {
	ungzPool  sync.Pool
	unlz4Pool sync.Pool

	bufferPool sync.Pool
	buf        *bytes.Buffer
}

func newDecompressor() *decompressor {
	return &decompressor{
		ungzPool: sync.Pool{
			New: func() interface{} {
				return new(gzip.Reader)
			},
		},
		unlz4Pool: sync.Pool{
			New: func() any { return lz4.NewReader(nil) },
		},
		bufferPool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
		buf: new(bytes.Buffer),
	}
}

func (d *decompressor) decompress(src []byte, codec byte) ([]byte, error) {
	switch codecType(codec) {
	case codecNone:
		return src, nil
	case codecLZ4:
		unlz4 := d.unlz4Pool.Get().(*lz4.Reader)
		defer d.unlz4Pool.Put(unlz4)
		unlz4.Reset(bytes.NewReader(src))

		out := new(bytes.Buffer)
		_, err := io.Copy(out, unlz4)
		if err != nil {
			return nil, err
		}

		return out.Bytes(), nil
	case codecGzip:
		ungz := d.ungzPool.Get().(*gzip.Reader)
		defer d.ungzPool.Put(ungz)
		if err := ungz.Reset(bytes.NewReader(src)); err != nil {
			return nil, err
		}

		out := new(bytes.Buffer)
		_, err := io.Copy(out, ungz)
		if err != nil {
			return nil, err
		}

		return out.Bytes(), nil
	default:
		return nil, errors.New("unsupported codec")
	}
}

func (d *decompressor) decompressWithBuf(src []byte, codec byte) ([]byte, error) {
	switch codecType(codec) {
	case codecNone:
		return src, nil
	case codecLZ4:
		unlz4 := d.unlz4Pool.Get().(*lz4.Reader)
		defer d.unlz4Pool.Put(unlz4)
		unlz4.Reset(bytes.NewReader(src))

		d.buf.Reset()

		_, err := io.Copy(d.buf, unlz4)
		if err != nil {
			return nil, err
		}

		return append([]byte(nil), d.buf.Bytes()...), nil

	case codecGzip:
		ungz := d.ungzPool.Get().(*gzip.Reader)
		defer d.ungzPool.Put(ungz)
		if err := ungz.Reset(bytes.NewReader(src)); err != nil {
			return nil, err
		}

		d.buf.Reset()

		_, err := io.Copy(d.buf, ungz)
		if err != nil {
			return nil, err
		}

		return append([]byte(nil), d.buf.Bytes()...), nil
	default:
		return nil, errors.New("unsupported codec")
	}
}

func (d *decompressor) decompressWithPooling(src []byte, codec byte) ([]byte, error) {
	switch codecType(codec) {
	case codecNone:
		return src, nil
	case codecLZ4:
		unlz4 := d.unlz4Pool.Get().(*lz4.Reader)
		defer d.unlz4Pool.Put(unlz4)
		unlz4.Reset(bytes.NewReader(src))

		out := d.bufferPool.Get().(*bytes.Buffer)
		defer func() {
			out.Reset()
			d.bufferPool.Put(out)
		}()

		_, err := io.Copy(out, unlz4)
		if err != nil {
			return nil, err
		}

		return append([]byte(nil), out.Bytes()...), nil

	case codecGzip:
		ungz := d.ungzPool.Get().(*gzip.Reader)
		defer d.ungzPool.Put(ungz)
		if err := ungz.Reset(bytes.NewReader(src)); err != nil {
			return nil, err
		}

		out := d.bufferPool.Get().(*bytes.Buffer)
		defer func() {
			out.Reset()
			d.bufferPool.Put(out)
		}()

		_, err := io.Copy(out, ungz)
		if err != nil {
			return nil, err
		}

		return append([]byte(nil), out.Bytes()...), nil

	default:
		return nil, errors.New("unsupported codec")
	}
}

// Dummy codecType and codec values for demonstration purposes
const (
	codecNone byte = iota
	codecGzip
	codecLZ4
)

func codecType(codec byte) byte {
	return codec
}
