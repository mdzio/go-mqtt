// Copyright (c) 2014 The SurgeMQ Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"bufio"
	"io"
	"sync"
	"sync/atomic"
)

var (
	bufcnt int64
)

const (
	defaultBufferSize     = 1024 * 256
	defaultReadBlockSize  = 8192
	defaultWriteBlockSize = 8192
)

type sequence struct {
	// The current position of the producer or consumer
	cursor int64

	// The previous known position of the consumer (if producer) or producer (if consumer)
	gate int64
}

func newSequence() *sequence {
	return &sequence{}
}

func (sq *sequence) get() int64 {
	return atomic.LoadInt64(&sq.cursor)
}

func (sq *sequence) set(seq int64) {
	atomic.StoreInt64(&sq.cursor, seq)
}

type buffer struct {
	id int64

	buf []byte
	tmp []byte

	size int64
	mask int64

	done int64

	pseq *sequence
	cseq *sequence

	pcond *sync.Cond
	ccond *sync.Cond

	cwait int64
	pwait int64
}

func newBuffer(size int64) (*buffer, error) {
	if size < 0 {
		return nil, bufio.ErrNegativeCount
	}

	// adjust buffer size
	if size == 0 {
		size = defaultBufferSize
	}
	if !powerOfTwo64(size) {
		size = roundUpPowerOfTwo64(size)
	}
	if size < 2*defaultReadBlockSize {
		size = 2 * defaultReadBlockSize
	}

	return &buffer{
		id:    atomic.AddInt64(&bufcnt, 1),
		buf:   make([]byte, size),
		size:  size,
		mask:  size - 1,
		pseq:  newSequence(),
		cseq:  newSequence(),
		pcond: sync.NewCond(new(sync.Mutex)),
		ccond: sync.NewCond(new(sync.Mutex)),
		cwait: 0,
		pwait: 0,
	}, nil
}

func (bf *buffer) ID() int64 {
	return bf.id
}

func (bf *buffer) Close() error {
	atomic.StoreInt64(&bf.done, 1)

	bf.pcond.L.Lock()
	bf.pcond.Broadcast()
	bf.pcond.L.Unlock()

	bf.pcond.L.Lock()
	bf.ccond.Broadcast()
	bf.pcond.L.Unlock()

	return nil
}

func (bf *buffer) Len() int {
	cpos := bf.cseq.get()
	ppos := bf.pseq.get()
	return int(ppos - cpos)
}

func (bf *buffer) ReadFrom(r io.Reader) (int64, error) {
	defer bf.Close()

	total := int64(0)

	for {
		if bf.isDone() {
			return total, io.EOF
		}

		start, cnt, err := bf.waitForWriteSpace(defaultReadBlockSize)
		if err != nil {
			return 0, err
		}

		pstart := start & bf.mask
		pend := pstart + int64(cnt)
		if pend > bf.size {
			pend = bf.size
		}

		n, err := r.Read(bf.buf[pstart:pend])

		if n > 0 {
			total += int64(n)
			_, err := bf.WriteCommit(n)
			if err != nil {
				return total, err
			}
		}

		if err != nil {
			return total, err
		}
	}
}

func (bf *buffer) WriteTo(w io.Writer) (int64, error) {
	defer bf.Close()

	total := int64(0)

	for {
		if bf.isDone() {
			return total, io.EOF
		}

		p, err := bf.ReadPeek(defaultWriteBlockSize)

		// There's some data, let's process it first
		if len(p) > 0 {
			n, err := w.Write(p)
			total += int64(n)
			//glog.Debugf("Wrote %d bytes, totaling %d bytes", n, total)

			if err != nil {
				return total, err
			}

			_, err = bf.ReadCommit(n)
			if err != nil {
				return total, err
			}
		}

		if err != ErrBufferInsufficientData && err != nil {
			return total, err
		}
	}
}

func (bf *buffer) Read(p []byte) (int, error) {
	if bf.isDone() && bf.Len() == 0 {
		//glog.Debugf("isDone and len = %d", this.Len())
		return 0, io.EOF
	}

	pl := int64(len(p))

	for {
		cpos := bf.cseq.get()
		ppos := bf.pseq.get()
		cindex := cpos & bf.mask

		// If consumer position is at least len(p) less than producer position, that means
		// we have enough data to fill p. There are two scenarios that could happen:
		// 1. cindex + len(p) < buffer size, in this case, we can just copy() data from
		//    buffer to p, and copy will just copy enough to fill p and stop.
		//    The number of bytes copied will be len(p).
		// 2. cindex + len(p) > buffer size, this means the data will wrap around to the
		//    the beginning of the buffer. In thise case, we can also just copy data from
		//    buffer to p, and copy will just copy until the end of the buffer and stop.
		//    The number of bytes will NOT be len(p) but less than that.
		if cpos+pl < ppos {
			n := copy(p, bf.buf[cindex:])

			bf.cseq.set(cpos + int64(n))
			bf.pcond.L.Lock()
			bf.pcond.Broadcast()
			bf.pcond.L.Unlock()

			return n, nil
		}

		// If we got here, that means there's not len(p) data available, but there might
		// still be data.

		// If cpos < ppos, that means there's at least ppos-cpos bytes to read. Let's just
		// send that back for now.
		if cpos < ppos {
			// n bytes available
			b := ppos - cpos

			// bytes copied
			var n int

			// if cindex+n < size, that means we can copy all n bytes into p.
			// No wrapping in this case.
			if cindex+b < bf.size {
				n = copy(p, bf.buf[cindex:cindex+b])
			} else {
				// If cindex+n >= size, that means we can copy to the end of buffer
				n = copy(p, bf.buf[cindex:])
			}

			bf.cseq.set(cpos + int64(n))
			bf.pcond.L.Lock()
			bf.pcond.Broadcast()
			bf.pcond.L.Unlock()
			return n, nil
		}

		// If we got here, that means cpos >= ppos, which means there's no data available.
		// If so, let's wait...

		bf.ccond.L.Lock()
		for ppos = bf.pseq.get(); cpos >= ppos; ppos = bf.pseq.get() {
			if bf.isDone() {
				return 0, io.EOF
			}

			bf.cwait++
			bf.ccond.Wait()
		}
		bf.ccond.L.Unlock()
	}
}

func (bf *buffer) Write(p []byte) (int, error) {
	if bf.isDone() {
		return 0, io.EOF
	}

	start, _, err := bf.waitForWriteSpace(len(p))
	if err != nil {
		return 0, err
	}

	// If we are here that means we now have enough space to write the full p.
	// Let's copy from p into this.buf, starting at position ppos&this.mask.
	total := ringCopy(bf.buf, p, int64(start)&bf.mask)

	bf.pseq.set(start + int64(len(p)))
	bf.ccond.L.Lock()
	bf.ccond.Broadcast()
	bf.ccond.L.Unlock()

	return total, nil
}

// Description below is copied completely from bufio.Peek()
//   http://golang.org/pkg/bufio/#Reader.Peek
// Peek returns the next n bytes without advancing the reader. The bytes stop being valid
// at the next read call. If Peek returns fewer than n bytes, it also returns an error
// explaining why the read is short. The error is bufio.ErrBufferFull if n is larger than
// b's buffer size.
// If there's not enough data to peek, error is ErrBufferInsufficientData.
// If n < 0, error is bufio.ErrNegativeCount
func (bf *buffer) ReadPeek(n int) ([]byte, error) {
	if int64(n) > bf.size {
		return nil, bufio.ErrBufferFull
	}

	if n < 0 {
		return nil, bufio.ErrNegativeCount
	}

	cpos := bf.cseq.get()
	ppos := bf.pseq.get()

	// If there's no data, then let's wait until there is some data
	bf.ccond.L.Lock()
	for ; cpos >= ppos; ppos = bf.pseq.get() {
		if bf.isDone() {
			return nil, io.EOF
		}

		bf.cwait++
		bf.ccond.Wait()
	}
	bf.ccond.L.Unlock()

	// m = the number of bytes available. If m is more than what's requested (n),
	// then we make m = n, basically peek max n bytes
	m := ppos - cpos
	err := error(nil)

	if m >= int64(n) {
		m = int64(n)
	} else {
		err = ErrBufferInsufficientData
	}

	// There's data to peek. The size of the data could be <= n.
	if cpos+m <= ppos {
		cindex := cpos & bf.mask

		// If cindex (index relative to buffer) + n is more than buffer size, that means
		// the data wrapped
		if cindex+m > bf.size {
			// reset the tmp buffer
			bf.tmp = bf.tmp[0:0]

			l := len(bf.buf[cindex:])
			bf.tmp = append(bf.tmp, bf.buf[cindex:]...)
			bf.tmp = append(bf.tmp, bf.buf[0:m-int64(l)]...)
			return bf.tmp, err
		}
		return bf.buf[cindex : cindex+m], err
	}

	return nil, ErrBufferInsufficientData
}

// Wait waits for for n bytes to be ready. If there's not enough data, then it will
// wait until there's enough. This differs from ReadPeek or Readin that Peek will
// return whatever is available and won't wait for full count.
func (bf *buffer) ReadWait(n int) ([]byte, error) {
	if int64(n) > bf.size {
		return nil, bufio.ErrBufferFull
	}

	if n < 0 {
		return nil, bufio.ErrNegativeCount
	}

	cpos := bf.cseq.get()
	ppos := bf.pseq.get()

	// This is the magic read-to position. The producer position must be equal or
	// greater than the next position we read to.
	next := cpos + int64(n)

	// If there's no data, then let's wait until there is some data
	bf.ccond.L.Lock()
	for ; next > ppos; ppos = bf.pseq.get() {
		if bf.isDone() {
			return nil, io.EOF
		}

		bf.ccond.Wait()
	}
	bf.ccond.L.Unlock()

	// If we are here that means we have at least n bytes of data available.
	cindex := cpos & bf.mask

	// If cindex (index relative to buffer) + n is more than buffer size, that means
	// the data wrapped
	if cindex+int64(n) > bf.size {
		// reset the tmp buffer
		bf.tmp = bf.tmp[0:0]

		l := len(bf.buf[cindex:])
		bf.tmp = append(bf.tmp, bf.buf[cindex:]...)
		bf.tmp = append(bf.tmp, bf.buf[0:n-l]...)
		return bf.tmp[:n], nil
	}

	return bf.buf[cindex : cindex+int64(n)], nil
}

// Commit moves the cursor forward by n bytes. It behaves like Read() except it doesn't
// return any data. If there's enough data, then the cursor will be moved forward and
// n will be returned. If there's not enough data, then the cursor will move forward
// as much as possible, then return the number of positions (bytes) moved.
func (bf *buffer) ReadCommit(n int) (int, error) {
	if int64(n) > bf.size {
		return 0, bufio.ErrBufferFull
	}

	if n < 0 {
		return 0, bufio.ErrNegativeCount
	}

	cpos := bf.cseq.get()
	ppos := bf.pseq.get()

	// If consumer position is at least n less than producer position, that means
	// we have enough data to fill p. There are two scenarios that could happen:
	// 1. cindex + n < buffer size, in this case, we can just copy() data from
	//    buffer to p, and copy will just copy enough to fill p and stop.
	//    The number of bytes copied will be len(p).
	// 2. cindex + n > buffer size, this means the data will wrap around to the
	//    the beginning of the buffer. In thise case, we can also just copy data from
	//    buffer to p, and copy will just copy until the end of the buffer and stop.
	//    The number of bytes will NOT be len(p) but less than that.
	if cpos+int64(n) <= ppos {
		bf.cseq.set(cpos + int64(n))
		bf.pcond.L.Lock()
		bf.pcond.Broadcast()
		bf.pcond.L.Unlock()
		return n, nil
	}

	return 0, ErrBufferInsufficientData
}

// WaitWrite waits for n bytes to be available in the buffer and then returns
// 1. the slice pointing to the location in the buffer to be filled
// 2. a boolean indicating whether the bytes available wraps around the ring
// 3. any errors encountered. If there's error then other return values are invalid
func (bf *buffer) WriteWait(n int) ([]byte, bool, error) {
	start, cnt, err := bf.waitForWriteSpace(n)
	if err != nil {
		return nil, false, err
	}

	pstart := start & bf.mask
	if pstart+int64(cnt) > bf.size {
		return bf.buf[pstart:], true, nil
	}

	return bf.buf[pstart : pstart+int64(cnt)], false, nil
}

func (bf *buffer) WriteCommit(n int) (int, error) {
	start, cnt, err := bf.waitForWriteSpace(n)
	if err != nil {
		return 0, err
	}

	// If we are here then there's enough bytes to commit
	bf.pseq.set(start + int64(cnt))

	bf.ccond.L.Lock()
	bf.ccond.Broadcast()
	bf.ccond.L.Unlock()

	return cnt, nil
}

func (bf *buffer) waitForWriteSpace(n int) (int64, int, error) {
	if bf.isDone() {
		return 0, 0, io.EOF
	}

	// The current producer position, remember it's a forever inreasing int64,
	// NOT the position relative to the buffer
	ppos := bf.pseq.get()

	// The next producer position we will get to if we write len(p)
	next := ppos + int64(n)

	// For the producer, gate is the previous consumer sequence.
	gate := bf.pseq.gate

	wrap := next - bf.size

	// If wrap point is greater than gate, that means the consumer hasn't read
	// some of the data in the buffer, and if we read in additional data and put
	// into the buffer, we would overwrite some of the unread data. It means we
	// cannot do anything until the customers have passed it. So we wait...
	//
	// Let's say size = 16, block = 4, ppos = 0, gate = 0
	//   then next = 4 (0+4), and wrap = -12 (4-16)
	//   _______________________________________________________________________
	//   | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 |
	//   -----------------------------------------------------------------------
	//    ^                ^
	//    ppos,            next
	//    gate
	//
	// So wrap (-12) > gate (0) = false, and gate (0) > ppos (0) = false also,
	// so we move on (no waiting)
	//
	// Now if we get to ppos = 14, gate = 12,
	// then next = 18 (4+14) and wrap = 2 (18-16)
	//
	// So wrap (2) > gate (12) = false, and gate (12) > ppos (14) = false aos,
	// so we move on again
	//
	// Now let's say we have ppos = 14, gate = 0 still (nothing read),
	// then next = 18 (4+14) and wrap = 2 (18-16)
	//
	// So wrap (2) > gate (0) = true, which means we have to wait because if we
	// put data into the slice to the wrap point, it would overwrite the 2 bytes
	// that are currently unread.
	//
	// Another scenario, let's say ppos = 100, gate = 80,
	// then next = 104 (100+4) and wrap = 88 (104-16)
	//
	// So wrap (88) > gate (80) = true, which means we have to wait because if we
	// put data into the slice to the wrap point, it would overwrite the 8 bytes
	// that are currently unread.
	//
	if wrap > gate || gate > ppos {
		var cpos int64
		bf.pcond.L.Lock()
		for cpos = bf.cseq.get(); wrap > cpos; cpos = bf.cseq.get() {
			if bf.isDone() {
				return 0, 0, io.EOF
			}

			bf.pwait++
			bf.pcond.Wait()
		}

		bf.pseq.gate = cpos
		bf.pcond.L.Unlock()
	}

	return ppos, n, nil
}

func (bf *buffer) isDone() bool {
	return atomic.LoadInt64(&bf.done) == 1
}

func ringCopy(dst, src []byte, start int64) int {
	n := len(src)

	i, l := 0, 0

	for n > 0 {
		l = copy(dst[start:], src[i:])
		i += l
		n -= l

		if n > 0 {
			start = 0
		}
	}

	return i
}

func powerOfTwo64(n int64) bool {
	return n != 0 && (n&(n-1)) == 0
}

func roundUpPowerOfTwo64(n int64) int64 {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++

	return n
}
