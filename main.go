package main

import (
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/pierrec/lz4"
	"github.com/valyala/bytebufferpool"
)

type decompressor struct {
	unlz4Pool sync.Pool

	bufferPool sync.Pool
	buf        *bytes.Buffer
}

func newDecompressor() *decompressor {
	return &decompressor{
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

		return d.buf.Bytes(), nil
	default:
		return nil, errors.New("unsupported codec")
	}
}

func (d *decompressor) decompressWithBufAndExtraCopy(src []byte, codec byte) ([]byte, error) {
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
		out.Reset()
		defer d.bufferPool.Put(out)

		_, err := io.Copy(out, unlz4)
		if err != nil {
			return nil, err
		}

		return append([]byte(nil), out.Bytes()...), nil

	default:
		return nil, errors.New("unsupported codec")
	}
}

// sliceWriter a reusable slice as an io.Writer
type sliceWriter struct{ inner []byte }

func (s *sliceWriter) Write(p []byte) (int, error) {
	s.inner = append(s.inner, p...)
	return len(p), nil
}

var sliceWriters = sync.Pool{New: func() any { r := make([]byte, 8<<10); return &sliceWriter{inner: r} }}

func (d *decompressor) decompressWithSliceWriter(src []byte, codec byte) ([]byte, error) {
	switch codecType(codec) {
	case codecNone:
		return src, nil
	case codecLZ4:
		unlz4 := d.unlz4Pool.Get().(*lz4.Reader)
		defer d.unlz4Pool.Put(unlz4)
		unlz4.Reset(bytes.NewReader(src))

		dst := sliceWriters.Get().(*sliceWriter)
		dst.inner = dst.inner[:0]
		defer sliceWriters.Put(dst)

		_, err := io.Copy(dst, unlz4)
		if err != nil {
			return nil, err
		}

		return append([]byte(nil), dst.inner...), nil

	default:
		return nil, errors.New("unsupported codec")
	}
}

func (d *decompressor) decompressWithBytebufferpool(src []byte, codec byte) ([]byte, error) {
	switch codecType(codec) {
	case codecNone:
		return src, nil
	case codecLZ4:
		unlz4 := d.unlz4Pool.Get().(*lz4.Reader)
		defer d.unlz4Pool.Put(unlz4)
		unlz4.Reset(bytes.NewReader(src))

		bb := bytebufferpool.Get()
		bb.Reset()
		defer bytebufferpool.Put(bb)

		_, err := io.Copy(bb, unlz4)
		if err != nil {
			return nil, err
		}

		return append([]byte(nil), bb.Bytes()...), nil

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
