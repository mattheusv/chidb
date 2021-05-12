package chidb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPageWriteReadHeader(t *testing.T) {
	db, err := os.CreateTemp(os.TempDir(), t.Name())
	require.Nil(t, err)

	pager, err := Open(db.Name())
	require.Nil(t, err)

	btree := DefaultBTreeHeader()
	writenHeader, err := btree.Bytes()
	require.Nil(t, err)

	err = pager.WriteHeader(writenHeader)
	require.Nil(t, err, "Expected nil error to write header: %v", err)

	readHeader, err := pager.ReadHeader()
	require.Nil(t, err)

	assert.Equal(t, HeaderSize, len(readHeader), "Expected equals header size")
	assert.Equal(t, writenHeader, readHeader, "Expected equals headers after write and read")
}
