package chidb_test

import (
	"os"
	"testing"

	"github.com/msAlcantara/chidb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPageWriteReadHeader(t *testing.T) {
	pager := openPager(t)

	btree := chidb.DefaultBTreeHeader()
	writenHeader, err := btree.Bytes()
	require.Nil(t, err)

	err = pager.WriteHeader(writenHeader)
	require.Nil(t, err, "Expected nil error to write header: %v", err)

	readHeader, err := pager.ReadHeader()
	require.Nil(t, err)

	assert.Equal(t, chidb.HeaderSize, len(readHeader), "Expected equals header size")
	assert.Equal(t, writenHeader, readHeader, "Expected equals headers after write and read")
}

func TestPageWriteReadPage(t *testing.T) {
	pager := openPager(t)

	nPage := pager.AllocatePage()

	page, err := pager.ReadPage(nPage)
	require.Nil(t, err)

	node := chidb.NewBTreeNode(page, chidb.LeafTable)

	nodeBytes, err := node.Bytes()
	require.Nil(t, err)

	err = page.Write(nodeBytes)
	require.Nil(t, err)

	err = pager.WritePage(page)
	require.Nil(t, err)
}

func openPager(tb testing.TB) *chidb.Pager {
	db, err := os.CreateTemp(os.TempDir(), tb.Name())
	require.Nil(tb, err)

	pager, err := chidb.Open(db.Name())
	require.Nil(tb, err)
	return pager
}
