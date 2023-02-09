// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package utils contains basic utilities for Tatris
package utils

import (
	"encoding/base64"
	"github.com/google/uuid"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"go.uber.org/zap"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

type Seq struct {
	m         *sync.Mutex
	seqNumber int32
}

func (s *Seq) IncreaseAndGetSeq(delta int32) int32 {
	s.m.Lock()
	s.seqNumber += delta
	defer s.m.Unlock()
	return s.seqNumber
}

func NewSeq(start int32) *Seq {
	return &Seq{m: new(sync.Mutex), seqNumber: start}
}

var (
	r          = rand.New(rand.NewSource(time.Now().UnixNano()))
	mu         sync.Mutex
	seq        = NewSeq(RandInt32())
	interfaces []net.Interface // cached list of interfaces
)

func GenerateID() (string, error) {
	docId, err := generateID()
	if err != nil {
		logger.Error("generate ID fail", zap.String("msg", err.Error()))
		return "", err
	}
	return docId, nil
}

// create docID like es
func generateID() (string, error) {
	now, _, err := uuid.GetTime()
	if err != nil {
		return "", err
	}
	var idBytes [15]byte
	sequenceId := uint32(seq.IncreaseAndGetSeq(1) & 0xffffff)
	idBytes[0] = uint8(sequenceId & 0xff)
	idBytes[1] = uint8((sequenceId >> 16) & 0xff)
	idBytes[2] = uint8((now >> 16) & 0xff)
	idBytes[3] = uint8((now >> 24) & 0xff)
	idBytes[4] = uint8((now >> 32) & 0xff)
	idBytes[5] = uint8((now >> 40) & 0xff)
	_, address := getHardwareInterface("")
	if nil == address {
		address = constructDummyMulticastAddress()
	}
	var macBytes [6]byte
	for i := 0; i < len(macBytes); i++ {
		macBytes[i] = RandByte() ^ address[i]
	}
	copy(idBytes[6:], macBytes[:])
	idBytes[12] = uint8((now >> 8) & 0xff)
	idBytes[13] = uint8(sequenceId >> 8 & 0xff)
	idBytes[14] = uint8(now & 0xff)
	return base64.URLEncoding.EncodeToString(idBytes[0:]), nil
}

func constructDummyMulticastAddress() []byte {
	var address [6]byte
	for i := 0; i < len(address); i++ {
		address[i] = RandByte()
	}
	address[0] |= byte(1)
	return address[0:]
}

// TimestampUUID returns a Version 1 UUID based on the current NodeID and clock sequence, and the current time.
func TimestampUUID() (string, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}
	return strings.ReplaceAll(id.String(), "-", ""), nil
}

// RandomUUID creates a new random UUID
func RandomUUID() (string, error) {
	id := uuid.New()
	return strings.ReplaceAll(id.String(), "-", ""), nil
}

func RandInt32() int32 {
	mu.Lock()
	res := r.Int31()
	defer mu.Unlock()
	return res
}

func RandByte() byte {
	mu.Lock()
	res := r.Uint32()
	defer mu.Unlock()
	return byte(res)
}

func getHardwareInterface(name string) (string, []byte) {
	if interfaces == nil {
		var err error
		interfaces, err = net.Interfaces()
		if err != nil {
			return "", nil
		}
	}
	for _, ifs := range interfaces {
		if len(ifs.HardwareAddr) >= 6 && (name == "" || name == ifs.Name) {
			return ifs.Name, ifs.HardwareAddr
		}
	}
	return "", nil
}
