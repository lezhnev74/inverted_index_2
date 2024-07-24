package file

import (
	"encoding/binary"
	"fmt"
	"github.com/blevesearch/vellum"
	"github.com/ronanh/intcomp"
	"os"
	"path"
	"time"
)

// Writer accepts terms and their values and pushes them in 2 files:
// terms file (fst) and values file (compressed ints).
type Writer struct {
	valuesFile *os.File
	fst        *vellum.Builder
	// name is the prefix for the filenames, used as a key for the inverted index segment
	name string
	// valuesOffset keeps the current offset in the values file to accept new compressed data
	// then the offset goes to the FST
	valuesOffset uint64
}

// Append writes out bytes immediately to the sink files
// terms must be sorted prior to the call.
func (w *Writer) Append(tv TermValues) (err error) {

	if w.valuesFile == nil { // direct mode
		err = w.fst.Insert(tv.Term, tv.Values[0])
		if err != nil {
			return fmt.Errorf("writer: fst insert: %w", err)
		}
		return nil
	}

	// Put the new offset to FST
	err = w.fst.Insert(tv.Term, w.valuesOffset)
	if err != nil {
		return fmt.Errorf("writer: fst insert: %w", err)
	}

	// Compress values
	compressed := intcomp.CompressUint64(tv.Values, nil)

	// Put values to the values file
	err = binary.Write(w.valuesFile, binary.LittleEndian, compressed)
	if err != nil {
		return fmt.Errorf("writer: fst insert: %w", err)
	}
	w.valuesOffset += uint64(binary.Size(compressed)) // todo size recalculated (use counting writer)

	return nil
}

func (w *Writer) Close() error {
	err1 := w.fst.Close() // terms file is closed transitively (todo hopefully)

	var err2 error
	if w.valuesFile != nil { // direct mode
		err2 = w.valuesFile.Close()
	}

	if err1 != nil {
		return err1
	}
	return err2
}

func (w *Writer) GetName() string {
	return w.name
}

// NewDirectWriter creates a single-file writer (only FST).
// Each term contains just a single segment value.
func NewDirectWriter(dir string) (w *Writer, err error) {
	key := fmt.Sprint(time.Now().UnixNano())

	termFile, err := os.Create(path.Join(dir, key+"_fst"))
	if err != nil {
		return nil, fmt.Errorf("writer: fst file: %w", err)
	}

	fst, err := vellum.New(termFile, nil)
	if err != nil {
		return nil, fmt.Errorf("writer: terms file: %w", err)
	}

	return &Writer{
		name: key,
		fst:  fst,
	}, nil
}

// NewWriter extends direct writer with a secondary value file.
// In this case FST contains value offsets in the file.
func NewWriter(dir string) (w *Writer, err error) {
	w, err = NewDirectWriter(dir)
	if err != nil {
		return
	}
	key := w.GetName()

	valuesFile, err := os.Create(path.Join(dir, key+"_val"))
	if err != nil {
		return nil, fmt.Errorf("writer: values file: %w", err)
	}
	w.valuesFile = valuesFile

	return
}
