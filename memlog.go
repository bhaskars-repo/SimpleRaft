package main

/*
   @Author: Bhaskar S
   @Blog:   https://www.polarsparc.com
   @Date:   22 Aug 2020
*/

import (
	"errors"
	"fmt"
)

type MemLog struct {
	Cache map[int]string
}

func (m MemLog) Open() error {
	return nil
}

func (m MemLog) Read(index int) (string, error) {
	return m.Cache[index], nil
}

func (m MemLog) Write(index int, entry string) error {
	_, ok := m.Cache[index]
	if !ok {
		m.Cache[index] = entry
		return nil
	}
	return errors.New(fmt.Sprintf("Entry already exists for index: %d", index))
}

func (m MemLog) Close() error {
	return nil
}
