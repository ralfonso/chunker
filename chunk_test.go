package chunker

import (
	"fmt"
	"os"
	"testing"
)

func TestChunks(t *testing.T) {
	files := []string{
		"test-10mib",
		"test-50mib",
		"test-50mib-1bytechange",
		"test-50mib-2bytechange",
		"test-200mib",
	}

	for _, file := range files {
		f, err := os.Open(fmt.Sprintf("testdata/%s", file))
		if err != nil {
			t.Error(err)
		}
		defer f.Close()
		outC, errC := Chunks(f)
		t.Log(file)
		for chunkInfo := range outC {
			t.Logf("%d-%d, %x",
				chunkInfo.Offset,
				chunkInfo.Offset+chunkInfo.Length-1,
				chunkInfo.Hash)
		}

		select {
		case err := <-errC:
			if err != nil {
				t.Error(err)
			}
		default:
		}
	}
}
