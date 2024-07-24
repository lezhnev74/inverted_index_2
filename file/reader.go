package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/blevesearch/vellum"
	"github.com/ronanh/intcomp"
	"io"
	"os"
	"path"
)

type Reader struct {
	fst            *vellum.FST
	valuesFile     *os.File
	valuesFileSize uint64
	prevTerm       []byte
	prevOffset     uint64
	fstIterator    *vellum.FSTIterator
	prevFstError   error
}

func (r *Reader) Next() (TermValues, error) {
	// FST contains file offsets in the values file.
	// At each offset there is a sequence of compressed bytes.
	// We need to know the size of the compressed run in the file,
	// in order to read it out to the buffer before decompressing.

	// check the result of the last execution:
	if errors.Is(r.prevFstError, vellum.ErrIteratorDone) {
		return TermValues{}, io.EOF
	}

	term, valuesOffset := r.prevTerm, r.prevOffset
	var runSize uint64

	// peek:
	r.prevFstError = r.fstIterator.Next()
	if r.prevFstError == nil {
		r.prevTerm, r.prevOffset = r.fstIterator.Current()
		r.prevTerm = append([]byte{}, r.prevTerm...) // copy from FST internal buffer
		runSize = r.prevOffset - valuesOffset
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

	tv := TermValues{term, []uint64{}}

	if r.valuesFile == nil {
		// direct mode
		tv.Values = []uint64{valuesOffset}
		return tv, nil
	}

	compressed := make([]uint64, runSize/8)
	_, err := r.valuesFile.Seek(int64(valuesOffset), io.SeekStart)
	if err != nil {
		return tv, fmt.Errorf("reader: values file: %w", err)
	}

	err = binary.Read(r.valuesFile, binary.LittleEndian, compressed)
	if err != nil {
		return tv, fmt.Errorf("reader: values file: decompress: %w", err)
	}

	tv.Values = intcomp.UncompressUint64(compressed, nil)

	return tv, nil
}

func (r *Reader) Close() error {
	err0 := r.fstIterator.Close()
	err1 := r.fst.Close()

	var err2 error
	if r.valuesFile != nil {
		err2 = r.valuesFile.Close()
	}

	if err0 != nil {
		return err0
	}

	if err1 != nil {
		return err1
	}

	return err2
}

// NewReader will open terms and value files and iterate over the term values
func NewReader(dir string, key string) (*Reader, error) {

	fstFilename := path.Join(dir, key+"_fst")
	fst, err := vellum.Open(fstFilename)
	if err != nil {
		return nil, fmt.Errorf("reader: terms file: %w", err)
	}

	fstIterator, err := fst.Iterator(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("reader: fst: %w", err)
	}
	firstTerm, firstOffset := fstIterator.Current()

	// here we can switch to "no-values" file where all FST terms contains the same single value
	// that is for the use-case where we ingest one segment's terms.
	valuesFilename := path.Join(dir, key+"_val")
	valuesFileSize := uint64(0)
	valuesFile, err := os.Open(valuesFilename)
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
	}

	r := &Reader{
		fst:            fst,
		fstIterator:    fstIterator,
		valuesFile:     valuesFile,
		valuesFileSize: valuesFileSize,
		prevTerm:       append([]byte{}, firstTerm...), // copy from FST internal buffer
		prevOffset:     firstOffset,
	}

	return r, nil
}
