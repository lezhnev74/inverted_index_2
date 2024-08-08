package file

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/blevesearch/vellum"
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/ronanh/intcomp"
	"golang.org/x/exp/mmap"
	"io"
	"os"
	"path"
)

// Reader reads terms and their values from underlying filesystem.
// It implements Iterator and can be combined with other iterators for efficient reading.
type Reader struct {
	fst            *vellum.FST
	valuesFile     *os.File
	valuesFileSize uint64
	prevTerm       []byte
	prevOffset     uint64
	fstIterator    *vellum.FSTIterator
	prevFstError   error
	maxTerm        []byte // right boundary, INCLUSIVE
	valuesMmap     *mmap.ReaderAt
	valuesBuf      []byte
}

func (r *Reader) Next() (TermValues, error) {
	// FST contains file offsets in the values file.
	// At each offset there is a sequence of compressed bytes.
	// We need to know the size of the compressed run in the file,
	// in order to read it out to the buffer before decompressing.

	// check the result of the last execution:
	if errors.Is(r.prevFstError, vellum.ErrIteratorDone) {
		return TermValues{}, go_iterators.EmptyIterator
	}

	term, valuesOffset := r.prevTerm, r.prevOffset
	var runSize uint64

	// peek:
	r.prevFstError = r.fstIterator.Next()
	if r.prevFstError == nil {
		r.prevTerm, r.prevOffset = r.fstIterator.Current()
		r.prevTerm = append([]byte{}, r.prevTerm...) // copy from FST internal buffer
		runSize = r.prevOffset - valuesOffset

		if r.maxTerm != nil && bytes.Compare(r.prevTerm, r.maxTerm) > 0 {
			// iterator returned a term that is greater than the right iteration boundary.
			// so we need to return the current term and stop on the next call.
			r.prevFstError = vellum.ErrIteratorDone
		}

	} else {
		// peek failed:
		if errors.Is(r.prevFstError, vellum.ErrIteratorDone) {
			// the run takes all file space that is left
			runSize = r.valuesFileSize - valuesOffset
		} else {
			// something bad happened
			return TermValues{}, fmt.Errorf("reader: fst iterator: %w", r.prevFstError)
		}
	}

	tv := TermValues{term, []uint32{}}

	if r.valuesFile == nil {
		// direct mode
		tv.Values = []uint32{uint32(valuesOffset)}
		return tv, nil
	}

	compressed := make([]uint32, runSize/4)

	_, err := r.valuesMmap.ReadAt(r.valuesBuf, int64(valuesOffset))
	if err != nil && !errors.Is(err, io.EOF) {
		return tv, fmt.Errorf("reader: values file: mmap: %w", err)
	}

	err = binary.Read(bytes.NewBuffer(r.valuesBuf), binary.LittleEndian, compressed)
	if err != nil {
		return tv, fmt.Errorf("reader: values file: decompress: %w", err)
	}

	tv.Values = intcomp.UncompressUint32(compressed, nil)

	return tv, nil
}

func (r *Reader) Close() error {
	err0 := r.fstIterator.Close()
	err1 := r.fst.Close()

	var err2 error
	if r.valuesMmap != nil {
		err2 = r.valuesMmap.Close()
	}

	var err3 error
	if r.valuesFile != nil {
		err3 = r.valuesFile.Close()
	}

	if err0 != nil {
		return err0
	}

	if err1 != nil {
		return err1
	}

	if err2 != nil {
		return err2
	}

	return err3
}

// NewReader will open terms and value files and iterate over the term values
// min, max (inclusive) scopes the internal terms iterator.
func NewReader(dir string, key string, min, max []byte) (*Reader, error) {

	fstFilename := path.Join(dir, key+"_fst")
	fst, err := vellum.Open(fstFilename)
	if err != nil {
		return nil, fmt.Errorf("reader: terms file: %w", err)
	}

	// do not set the right bound to "max", we will do it manually.
	// as FST iterator won't return anything after max, so we can't
	// calculate the correct values offset because of that.
	fstIterator, err := fst.Iterator(min, nil)
	if err != nil {
		return nil, fmt.Errorf("reader: fst: %w", err)
	}
	firstTerm, firstOffset := fstIterator.Current()

	// here we can switch to "no-values" file where all FST terms contains the same single value
	// that is for the use-case where we ingest one segment's terms.
	valuesFilename := path.Join(dir, key+"_val")
	valuesFileSize := uint64(0)
	valuesFile, err := os.Open(valuesFilename)
	var mmapReader *mmap.ReaderAt
	if !errors.Is(err, os.ErrNotExist) {
		// direct mode enabled if no values file found
		if err != nil {
			return nil, fmt.Errorf("reader: value file: %w", err)
		}
		fInfo, err := valuesFile.Stat()
		if err != nil {
			return nil, fmt.Errorf("reader: value file: %w", err)
		}
		valuesFileSize = uint64(fInfo.Size())

		mmapReader, err = mmap.Open(valuesFilename)
		if err != nil {
			return nil, fmt.Errorf("reader: value file: %w", err)
		}
	}

	r := &Reader{
		fst:            fst,
		fstIterator:    fstIterator,
		valuesFile:     valuesFile,
		valuesFileSize: valuesFileSize,
		prevTerm:       append([]byte{}, firstTerm...), // copy from FST internal buffer
		prevOffset:     firstOffset,
		maxTerm:        max,
		valuesMmap:     mmapReader,
		valuesBuf:      make([]byte, 4096),
	}

	return r, nil
}
