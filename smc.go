package smc

import (
	"encoding/binary"
	"errors"
	"math"
)

var (
	n1 = uint64(math.Pow(2, 7))
	n2 = uint64(math.Pow(2, 14))
	n3 = uint64(math.Pow(2, 21))
	n4 = uint64(math.Pow(2, 28))
	n5 = uint64(math.Pow(2, 35))
	n6 = uint64(math.Pow(2, 42))
	n7 = uint64(math.Pow(2, 49))
	n8 = uint64(math.Pow(2, 56))
	n9 = uint64(math.Pow(2, 63))
)

func encodingLength(i uint64) uint64 {
	if i < n1 {
		return 1
	} else if i < n2 {
		return 2
	} else if i < n3 {
		return 3
	} else if i < n4 {
		return 4
	} else if i < n5 {
		return 5
	} else if i < n6 {
		return 6
	} else if i < n7 {
		return 7
	} else if i < n8 {
		return 8
	} else if i < n9 {
		return 9
	}
	return 10
}

const defaultMaxSize = 8 * 1024 * 1024

type SMC struct {
	state     int
	factor    uint64
	varint    uint64
	header    uint64
	message   []byte
	destroyed bool
	receiving bool
	length    int
	consumed  int
	err       error
	onmissing func(int)
	onmessage func(uint64, uint8, []byte, []byte, int)
}

type SMCOption = func(*SMC)

func WithMissingHandler(f func(int)) SMCOption {
	return func(s *SMC) {
		s.onmissing = f
	}
}

func WithMessageHandler(f func(uint64, uint8, []byte, []byte, int)) SMCOption {
	return func(s *SMC) {
		s.onmessage = f
	}
}

func NewSMC(opts ...SMCOption) *SMC {
	s := &SMC{
		factor: 1,
	}

	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *SMC) Error() error {
	err := s.err
	s.err = nil

	return err
}

func (s *SMC) destroy(err error) {
	if err != nil {
		s.err = err
	}

	s.destroyed = true
}

func (s *SMC) next(data []byte, offset int) bool {
	switch s.state {
	case 0:
		s.state = 1
		s.factor = 1
		s.length = int(s.varint)
		s.consumed = 0
		s.varint = 0

		if s.length == 0 {
			s.state = 0
		}

		return true
	case 1:
		s.state = 2
		s.factor = 1
		s.header = s.varint
		s.length -= s.consumed
		s.consumed = 0
		s.varint = 0

		if s.length < 0 || s.length > defaultMaxSize {
			s.destroy(ErrSizeLimit)

			return false
		}

		if s.onmissing != nil {
			extra := len(data) - offset
			if s.length > extra {
				s.onmissing(s.length - extra)
			}
		}

		return true
	case 2:
		s.state = 0

		if s.onmessage != nil {
			s.onmessage(s.header>>4, uint8(s.header&0b1111), s.message, data, offset)
		}

		s.message = nil

		return !s.destroyed
	default:
		return false
	}
}

var (
	ErrInvalidVarint = errors.New("smc: incoming varint is invalid")
	ErrSizeLimit     = errors.New("smc: incoming message is larger than max size")
)

func (s *SMC) readVarint(data []byte, offset int) int {
	for ; offset < len(data); offset++ {
		s.varint += uint64(data[offset]&127) * s.factor
		s.consumed += 1
		if data[offset] < 128 {
			offset += 1

			if s.next(data, offset) {
				return offset
			}

			return len(data)
		}

		s.factor *= 128
	}

	if s.consumed >= 9 {
		s.destroy(ErrInvalidVarint) // 9 * 7bits is 63 ie the safest max
	}

	return len(data)
}

func (s *SMC) readMessage(data []byte, offset int) int {
	l := len(data)

	free := l - offset
	if free >= s.length {
		if s.message != nil {
			copy(s.message[len(s.message)-s.length:], data[offset:])
		} else {
			s.message = data[offset : offset+s.length]
		}

		offset += s.length

		if s.next(data, offset) {
			return offset
		}

		return l
	}

	if s.message == nil {
		s.message = make([]byte, s.length)
	}

	copy(s.message[len(s.message)-s.length:], data[offset:])

	s.length -= free

	return l
}

func (s *SMC) Send(ch uint64, k uint8, d []byte) []byte {
	header := ch<<4 | uint64(k)
	length := uint64(len(d)) + encodingLength(header)
	payload := make([]byte, encodingLength(uint64(length))+length)

	n := binary.PutUvarint(payload, length)
	n += binary.PutUvarint(payload[n:], header)

	copy(payload[n:], d)

	return payload
}

func (s *SMC) Recv(d []byte) bool {
	l := len(d)

	if l == 0 {
		return !s.destroyed
	}

	if s.receiving {
		panic("smc: cannot recursively receive data")
	}

	s.receiving = true

	offset := 0

	for offset < l {
		if s.state == 2 {
			offset = s.readMessage(d, offset)
		} else {
			offset = s.readVarint(d, offset)
		}
	}

	if s.state == 2 && s.length == 0 {
		s.readMessage(d, offset)
	}

	s.receiving = false

	return !s.destroyed
}

type Message struct {
	ID      int
	Channel rune
	Data    []byte
}

func NewMessage(id int, ch rune, data []byte) *Message {
	return &Message{
		ID:      id,
		Channel: ch,
		Data:    data,
	}
}

func (s *SMC) SendBatch(items []*Message) []byte {
	offset := 0

	var length uint64

	for _, v := range items {
		// 16 is >= the max size of the varints
		length += 16 + encodingLength(uint64(len(v.Data)))
	}

	payload := make([]byte, length)

	for _, v := range items {
		header := uint64(v.ID<<4) | uint64(v.Channel)
		l := uint64(len(v.Data)) + encodingLength(header)

		offset += binary.PutUvarint(payload[offset:], l)
		offset += binary.PutUvarint(payload[offset:], header)

		copy(payload[offset:], v.Data)

		offset += len(v.Data)
	}

	return payload[0:offset]
}
