package file

import (
	"encoding/binary"
	"errors"
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
	basedir    string
	valuesFile *os.File
	fst        *vellum.Builder
	// key is the prefix for the filenames, used as a key for the inverted index segment
	key string
	// valuesOffset keeps the current offset in the values file to accept new compressed data
	// then the offset goes to the FST
	valuesOffset uint64
}

func (w *Writer) GetFst() *vellum.Builder { return w.fst }

// Append writes out bytes immediately to the sink files
// terms must be sorted prior to the call.
func (w *Writer) Append(tv TermValues) (err error) {

	if w.valuesFile == nil { // direct mode
		err = w.fst.Insert(tv.Term, uint64(tv.Values[0]))
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
	compressed := intcomp.CompressUint32(tv.Values, nil)

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
	w.fst = nil

	var err2 error
	if w.valuesFile != nil { // direct mode
		err2 = w.valuesFile.Close()
	}

	if err1 != nil {
		return err1
	}

	if err2 != nil {
		return err2
	}

	// rename files to make them visible for readers
	os.Rename(
		path.Join(w.basedir, w.key+"_fst_tmp"),
		path.Join(w.basedir, w.key+"_fst"),
	)
	os.Rename(
		path.Join(w.basedir, w.key+"_val_tmp"),
		path.Join(w.basedir, w.key+"_val"),
	)

	return nil
}

func (w *Writer) GetKey() string {
	return w.key
}

// NewDirectWriter creates a single-file writer (only FST).
// Each term contains just a single segment value.
func NewDirectWriter(dir string, fst *vellum.Builder) (w *Writer, err error) {
	key := fmt.Sprint(time.Now().UnixNano())

	termFile, err := os.Create(path.Join(dir, key+"_fst_tmp"))
	if err != nil {
		return nil, fmt.Errorf("writer: terms file: %w", err)
	}

	if fst == nil {
		fst, err = vellum.New(termFile, nil)
	} else {
		err = fst.Reset(termFile)
	}
	if err != nil {
		return nil, fmt.Errorf("writer: fst: %w", err)
	}

	return &Writer{
		basedir: dir,
		key:     key,
		fst:     fst,
	}, nil
}

// NewWriter extends direct writer with a secondary value file.
// In this case FST contains value offsets in the file.
func NewWriter(dir string, fst *vellum.Builder) (w *Writer, err error) {
	w, err = NewDirectWriter(dir, fst)
	if err != nil {
		return
	}
	key := w.GetKey()

	valuesFile, err := os.Create(path.Join(dir, key+"_val_tmp"))
	if err != nil {
		return nil, fmt.Errorf("writer: values file: %w", err)
	}
	w.valuesFile = valuesFile

	return
}

// RemoveSegment unlinks all associated files for the given segment key
func RemoveSegment(dir, key string) error {
	err1 := os.Remove(path.Join(dir, key+"_fst"))
	err2 := os.Remove(path.Join(dir, key+"_val"))
	if !errors.Is(err1, os.ErrNotExist) {
		return err1
	}
	return err2
}
