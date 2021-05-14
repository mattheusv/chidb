package chidb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBTreeFirtNodePageLeafTable(t *testing.T) {
	btree := openBtree(t)

	node, err := btree.GetNodeByPage(1)
	require.Nil(t, err, "Expected nil error to get first node page")

	assert.Equal(t, LeafTable, node.Type())
}

func TestWriteFirstNodeNotOverrideFileHeader(t *testing.T) {
}

func TestCreateNewNode(t *testing.T) {
	btree := openBtree(t)

	node, err := btree.NewNode(InternalTable)
	require.Nil(t, err, "Expected nil error to create new node")

	assert.Equal(t, uint32(2), node.page.number, "Expected equal page number")
	assert.Equal(t, InternalTable, node.typ, "Expected equal node type")
	assert.Equal(t, PageHeaderSize+uint16(1), node.freeOffset, "Expected equal free offset")
	assert.Equal(t, uint16(0), node.nCells, "Expected equal number cells")
	assert.Equal(t, uint16(PageSize), node.cellsOffset, "Expected equal cells offset")
	assert.Equal(t, uint16(0), node.rightPage, "Expected equal right page")
	assert.Equal(t, byte(PageHeaderSize), node.cellOffsetArray, "Expected equal cell offset array")

	newNode, err := btree.GetNodeByPage(node.page.number)
	require.Nil(t, err, "Expected nil error to get new node created")

	assert.Equal(t, node.page.number, newNode.page.number, "Expected equal page number after read from disk")
	assert.Equal(t, node.typ, newNode.typ, "Expected equal node type after read from disk")
	assert.Equal(t, node.freeOffset, newNode.freeOffset, "Expected equal free offset after read from disk")
	assert.Equal(t, node.nCells, newNode.nCells, "Expected equal number cell after read from disks")
	assert.Equal(t, node.cellsOffset, newNode.cellsOffset, "Expected equal cells offse after read from diskt")
	assert.Equal(t, node.rightPage, newNode.rightPage, "Expected equal right page after read from disk")
	assert.Equal(t, node.cellOffsetArray, newNode.cellOffsetArray, "Expected equal cell offset array after read from disk")

}

func TestBTreeOpen(t *testing.T) {
	invalidDb, err := os.CreateTemp(os.TempDir(), t.Name())
	require.Nil(t, err)
	_, err = invalidDb.WriteString("Invalid Header")
	require.Nil(t, err)

	db, err := os.CreateTemp(os.TempDir(), t.Name())
	require.Nil(t, err)

	testcases := []struct {
		name string
		db   string
		err  error
	}{
		{
			name: "TestOpenEmptyFile",
			db:   db.Name(),
			err:  nil,
		},
		{
			name: "TestOpenFile",
			db:   db.Name(),
			err:  nil,
		},
		{
			name: "TestOpenInvalidFile",
			db:   invalidDb.Name(),
			err:  ErrCorruptHeader,
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Open(tt.db)
			assert.Equal(t, tt.err, err)
		})
	}
}

func openBtree(tb testing.TB) *BTree {
	db, err := os.CreateTemp(os.TempDir(), tb.Name())
	require.Nil(tb, err)

	btree, err := Open(db.Name())
	require.Nil(tb, err)
	return btree
}
