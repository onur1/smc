package smc_test

import (
	"math"
	"testing"

	"github.com/onur1/smc"
	"github.com/stretchr/testify/assert"
)

func TestBasic(t *testing.T) {
	onmessage := func(ch uint64, k uint8, d []byte, _ []byte, _ int) {
		assert.EqualValues(t, ch, 0)
		assert.EqualValues(t, k, 1)
		assert.EqualValues(t, d, []byte("hi"))
	}

	a := smc.NewSMC(smc.WithMessageHandler(onmessage))

	a.Recv(a.Send(0, 1, []byte("hi")))
}

func TestBasicChunked(t *testing.T) {
	onmessage := func(ch uint64, k uint8, d []byte, _ []byte, _ int) {
		assert.EqualValues(t, ch, 0)
		assert.EqualValues(t, k, 1)
		assert.EqualValues(t, d, []byte("hi"))
	}

	a := smc.NewSMC(smc.WithMessageHandler(onmessage))

	payload := a.Send(0, 1, []byte("hi"))

	for i := 0; i < len(payload); i++ {
		a.Recv(payload[i : i+1])
	}
}

type expected struct {
	ch uint64
	k  uint64
	d  []byte
}

func TestTwoMessagesChunked(t *testing.T) {
	xs := []expected{
		{0, 1, []byte("hi")},
		{42, 3, []byte("hey")},
	}

	onmessage := func(ch uint64, k uint8, d []byte, _ []byte, _ int) {
		var e expected

		e, xs = xs[0], xs[1:]

		assert.EqualValues(t, ch, e.ch)
		assert.EqualValues(t, k, e.k)
		assert.EqualValues(t, d, e.d)
	}

	a := smc.NewSMC(smc.WithMessageHandler(onmessage))

	payload := a.Send(0, 1, []byte("hi"))

	for i := 0; i < len(payload); i++ {
		a.Recv(payload[i : i+1])
	}

	payload2 := a.Send(42, 3, []byte("hey"))

	for i := 0; i < len(payload2); i++ {
		a.Recv(payload2[i : i+1])
	}
}

func magicByteArray(size int) []byte {
	x := make([]byte, size)

	for i, _ := range x {
		x[i] = 0x20
	}

	return x
}

func TestTwoBigMessagesChunked(t *testing.T) {
	xs := []expected{
		{0, 1, magicByteArray(1e5)},
		{42, 3, magicByteArray(2e5)},
	}

	onmessage := func(ch uint64, k uint8, d []byte, _ []byte, _ int) {
		var e expected

		e, xs = xs[0], xs[1:]

		assert.EqualValues(t, ch, e.ch)
		assert.EqualValues(t, k, e.k)
		assert.EqualValues(t, d, e.d)
	}

	a := smc.NewSMC(smc.WithMessageHandler(onmessage))

	payload := a.Send(0, 1, magicByteArray(1e5))

	for i := 0; i < len(payload); i += 500 {
		a.Recv(payload[i:int(math.Min(float64(i+500), float64(len(payload))))])
	}

	payload2 := a.Send(42, 3, magicByteArray(2e5))

	for i := 0; i < len(payload2); i += 500 {
		a.Recv(payload2[i:int(math.Min(float64(i+500), float64(len(payload2))))])
	}
}

func TestEmptyMessage(t *testing.T) {
	onmessage := func(ch uint64, k uint8, d []byte, _ []byte, _ int) {
		assert.EqualValues(t, ch, 0)
		assert.EqualValues(t, k, 0)
		assert.EqualValues(t, d, []byte{})
	}

	a := smc.NewSMC(smc.WithMessageHandler(onmessage))

	a.Recv(a.Send(0, 0, []byte{}))
}

func TestChunkMessageIsCorrect(t *testing.T) {
	onmessage := func(ch uint64, k uint8, d []byte, _ []byte, _ int) {
		assert.EqualValues(t, ch, 0)
		assert.EqualValues(t, k, 1)
		assert.EqualValues(t, d, []byte("aaaaaaaaaa"))
	}

	onmissing := func(l int) {
		assert.Equal(t, l, 8)
	}

	a := smc.NewSMC(smc.WithMessageHandler(onmessage), smc.WithMissingHandler(onmissing))

	b := smc.NewSMC()

	batch := b.SendBatch([]*smc.Message{
		{
			Chan: 0,
			Cmd:  1,
			Data: []byte("aaaaaaaaaa"),
		},
	})

	a.Recv(batch[0:4])
	a.Recv(batch[4:12])
}
