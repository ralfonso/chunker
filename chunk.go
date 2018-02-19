package chunker

import (
	"crypto/sha512"
	"fmt"
	"hash"
	"io"

	"github.com/chmduquesne/rollinghash/buzhash64"
)

type chunker struct {
	rdr io.Reader

	rollH     *buzhash64.Buzhash64
	completeH hash.Hash
	chunkH    hash.Hash
	multiH    io.Writer

	buf []byte

	boundaryOffset int
	outC           chan ChunkInfo
	errC           chan error
}

type ChunkInfo struct {
	Offset int
	Length int
	Hash   []byte
}

func newChunker(rdr io.Reader) *chunker {
	rollH := buzhash64.New()
	completeH := sha512.New512_256()
	chunkH := sha512.New512_256()

	return &chunker{
		rdr: rdr,

		rollH:     rollH,
		completeH: completeH,
		chunkH:    chunkH,
		multiH:    io.MultiWriter(completeH, chunkH),

		buf:  make([]byte, 1024),
		outC: make(chan ChunkInfo, 1),
		errC: make(chan error, 1),
	}
}

func Chunks(rdr io.Reader) (<-chan ChunkInfo, <-chan error) {
	c := newChunker(rdr)
	go func() {
		defer func() {
			close(c.outC)
			close(c.errC)
		}()
		var (
			n         int
			err       error
			rerr      error
			total     int
			chunkSize int
		)

		buf1 := make([]byte, 16)
		n, rerr = io.ReadFull(c.rdr, buf1)
		if rerr != nil && rerr != io.EOF {
			c.errC <- err
			return
		}
		_, err = c.populateWindow()
		if err != nil {
			c.errC <- err
			return
		}
		total = n
		chunkSize = n
		if isBoundary(c.rollH) {
			c.makeChunk(n)
			chunkSize = 0
		}
		if rerr == io.EOF {
			if n > 0 {
				c.makeChunk(n)
			}
		}

		for {
			n, rerr = c.rdr.Read(c.buf)
			if rerr != nil && rerr != io.EOF {
				c.errC <- err
				return
			}

			for i := 0; i < n; i++ {
				_, err = c.multiH.Write(c.buf[i : i+1])
				if err != nil {
					c.errC <- err
					return
				}

				c.rollH.Roll(c.buf[i])
				total++
				chunkSize++

				if isBoundary(c.rollH) {
					c.makeChunk(chunkSize)
					chunkSize = 0
				}
			}

			if rerr == io.EOF {
				if chunkSize > 0 {
					c.makeChunk(chunkSize)
				}
				fmt.Printf("avg chunk size: %f\n", avg(chunkSizes))
				break
			}
		}

		return
	}()

	return c.outC, c.errC
}

func (c *chunker) populateWindow() (int, error) {
	ws := 16
	_, err := c.multiH.Write(c.buf[0:ws])
	if err != nil {
		return ws, err
	}
	_, err = c.rollH.Write(c.buf[0:ws])
	if err != nil {
		return ws, err
	}

	return ws, nil
}

func isBoundary(r hash.Hash) bool {
	s := r.Sum(nil)
	if s[7] > 0 || s[6] > 0 || s[5] > 0 {
		return false
	}

	return true
}

var (
	chunkSizes = make([]int, 0)
)

func (c *chunker) makeChunk(length int) {
	offset := c.boundaryOffset + 1
	c.outC <- ChunkInfo{
		Offset: offset,
		Length: length,
		Hash:   c.chunkH.Sum(nil),
	}
	c.boundaryOffset = offset + length - 1
	c.chunkH.Reset()
	chunkSizes = append(chunkSizes, length)
}

func avg(vs []int) float64 {
	var sum int
	for _, n := range vs {
		sum += n
	}

	return float64(sum) / float64(len(vs))
}
