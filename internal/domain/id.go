package domain

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"time"
)

// ID is a canonical lowercase UUID string.
type ID string

func NewID() ID {
	var b [16]byte
	ms := uint64(time.Now().UnixMilli())
	binary.BigEndian.PutUint64(b[0:8], ms<<16)
	if _, err := rand.Read(b[6:]); err != nil {
		panic("domain: entropy source unavailable")
	}
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80
	return ID(formatUUID(b))
}

func ParseID(s string) (ID, error) {
	if len(s) != 36 {
		return "", invalid("id", "must be a UUID")
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return "", invalid("id", "must be a UUID")
			}
		default:
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
				return "", invalid("id", "must be a lowercase UUID")
			}
		}
	}
	return ID(s), nil
}

func (id ID) String() string { return string(id) }

func (id ID) IsZero() bool { return id == "" }

func formatUUID(b [16]byte) string {
	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:], b[10:])
	return string(out[:])
}
