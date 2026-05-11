package btree

import (
	"bytes"
	"io"

	"btree-engine/internal/buffer"
	"btree-engine/internal/mvcc"
)

// BTreeCursor provides an iterator over a key range in the B+Tree.
type BTreeCursor struct {
	engine  *BPlusTree
	txn     *mvcc.Transaction
	current *buffer.Frame
	slotIdx int
	end     []byte // inclusive upper bound (nil = no bound)
}

// Scan returns a cursor positioned at the first key >= start.
func (bt *BPlusTree) Scan(txn *mvcc.Transaction, start, end []byte) (*BTreeCursor, error) {
	frame, parents, err := bt.findLeafWithParents(start)
	if err != nil {
		return nil, err
	}
	// Unpin parent chain — we only need the leaf
	for _, pf := range parents {
		bt.bufPool.UnpinPage(pf.PageID, false)
	}

	// Find the first slot >= start
	p := bt.bufPool.ReadPageData(frame)
	slotIdx := p.BinarySearch(start, mvcc.TupleKeyExtract)
	if slotIdx < 0 {
		slotIdx = -(slotIdx + 1)
	}

	return &BTreeCursor{
		engine:  bt,
		txn:     txn,
		current: frame,
		slotIdx: slotIdx,
		end:     end,
	}, nil
}

// Next advances the cursor and returns the next visible tuple.
// Returns (nil, io.EOF) when exhausted.
func (c *BTreeCursor) Next() (*mvcc.MVCCTuple, error) {
	for {
		p := c.engine.bufPool.ReadPageData(c.current)
		if c.slotIdx >= p.NumSlots() {
			// Advance to the next sibling page
			nextID := p.NextSib()
			c.engine.bufPool.UnpinPage(c.current.PageID, false)
			c.current = nil // clear before any early return to prevent double-unpin
			if nextID == 0 {
				return nil, io.EOF
			}
			frame, err := c.engine.bufPool.FetchPage(nextID)
			if err != nil {
				return nil, err
			}
			c.current = frame
			c.slotIdx = 0
			continue
		}

		data := p.GetRecord(c.slotIdx)
		c.slotIdx++
		if data == nil {
			continue // tombstone
		}
		t, err := mvcc.DecodeTuple(data)
		if err != nil {
			continue
		}
		// Check upper bound
		if c.end != nil && bytes.Compare(t.Key, c.end) > 0 {
			c.engine.bufPool.UnpinPage(c.current.PageID, false)
			c.current = nil // prevent double-unpin if Close() is called after EOF
			return nil, io.EOF
		}
		// Apply visibility
		if !c.txn.Snapshot.IsVisible(t, c.engine.tm) {
			continue
		}
		// IsVisible already handles Xmax via snapshot rules.
		// If IsVisible returned true with Xmax != 0, the deletion is not yet visible
		// to this snapshot — so the row is alive from our perspective.
		return t, nil
	}
}

// Close unpins the current leaf page.
func (c *BTreeCursor) Close() {
	if c.current != nil {
		c.engine.bufPool.UnpinPage(c.current.PageID, false)
		c.current = nil
	}
}
