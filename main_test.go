package main

import (
	"bytes"
	"testing"

	"github.com/pierrec/lz4"
)

func BenchmarkDecompress(b *testing.B) {
	d := newDecompressor()

	// Compress some data with gzip for the benchmark
	var buf bytes.Buffer
	lz := lz4.NewWriter(&buf)

	for i := 0; i < 100000; i++ {
		_, err := lz.Write([]byte(`{"placed_by": "XXXXXX","order_id": "100000000000000","exchange_order_id": "200000000000000","parent_order_id": null,"status": "CANCELLED","status_message": null,"status_message_raw": null,"order_timestamp": "2021-05-31 09:18:57","exchange_update_timestamp": "2021-05-31 09:18:58","exchange_timestamp": "2021-05-31 09:15:38","variety": "regular","modified": false,"exchange": "CDS","tradingsymbol": "USDINR21JUNFUT","instrument_token": 412675,"order_type": "LIMIT","transaction_type": "BUY","validity": "DAY","product": "NRML","quantity": 1,"disclosed_quantity": 0,"price": 72,"trigger_price": 0,"average_price": 0,"filled_quantity": 0,"pending_quantity": 1,"cancelled_quantity": 1,"market_protection": 0,"meta": {},"tag": null,"guid": "XXXXX"}`))
		if err != nil {
			b.Fatal(err)
		}
	}
	if err := lz.Close(); err != nil {
		b.Fatal(err)
	}
	compressedData := buf.Bytes()

	b.Run("without-buf", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, err := d.decompress(compressedData, codecLZ4)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("with-buf", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, err := d.decompressWithBuf(compressedData, codecLZ4)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("with-buf-extra-copy", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, err := d.decompressWithBufAndExtraCopy(compressedData, codecLZ4)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("with-pool", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, err := d.decompressWithPooling(compressedData, codecLZ4)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
